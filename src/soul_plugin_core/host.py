"""宿主：按实例计划激活，并在 start 成功后提交该实例暂存的服务。"""

import asyncio
from collections.abc import Iterable, Mapping
from pathlib import Path

from soul_plugin_core.capabilities import CapabilityPolicy
from soul_plugin_core.config import validate_deployment
from soul_plugin_core.context import ActivationScope, PluginContext
from soul_plugin_core.contracts import PluginDefinition
from soul_plugin_core.dependency import ActivationPlan
from soul_plugin_core.errors import PluginActivationError, PluginCleanupError, PluginLoadError
from soul_plugin_core.events import EventRegistry
from soul_plugin_core.instances import InstanceDefinition, expand_instance_plan
from soul_plugin_core.lifecycle import Fiber, FiberState, load_plugin
from soul_plugin_core.services import ServiceRegistry


class PluginHost:
    """按计划管理插件实例、生命周期事务及其拥有的资源。

    构造时展开实例计划；配置和能力在激活时统一预检，成功后才导入入口。
    """

    def __init__(
        self,
        definitions: Iterable[PluginDefinition],
        plan: ActivationPlan,
        *,
        config_path: str | Path,
        instances: Iterable[InstanceDefinition] | None = None,
        capability_policy: CapabilityPolicy | None = None,
    ) -> None:
        """先展开实例计划。不读取配置，也不导入插件。"""
        stored = tuple(definitions)
        by_id = {definition.plugin_id: definition for definition in stored}
        self._definitions = by_id
        self._plan = plan
        self._instance_plan = expand_instance_plan(stored, plan, instances)
        self._plugin_ids = dict(self._instance_plan.plugin_ids)
        self._config_path = Path(config_path)
        self._fibers = {
            instance_id: Fiber(
                instance_id=instance_id,
                plugin_id=self._plugin_ids[instance_id],
                source=by_id[self._plugin_ids[instance_id]].source,
            )
            for instance_id in self._instance_plan.order
        }
        self._configs: dict[str, Mapping[str, object]] | None = None
        if capability_policy is not None and not isinstance(capability_policy, CapabilityPolicy):
            raise TypeError("capability_policy must be a CapabilityPolicy")
        self._capability_policy = CapabilityPolicy(
            capability_policy.grants if capability_policy is not None else None
        )
        self._capabilities: dict[str, frozenset[str]] | None = None
        self._services = ServiceRegistry(self._definitions, self._instance_plan)
        self._events = EventRegistry()
        self._lock = asyncio.Lock()

    def get_state(self, instance_id: str) -> FiberState:
        """返回实例的当前状态；未知实例由映射抛出 KeyError。"""
        return self._fibers[instance_id].state

    async def activate(self) -> None:
        """按依赖顺序激活；失败或取消时逆序回滚本次新激活的实例。"""
        async with self._lock:
            if any(fiber.state is FiberState.DISPOSED for fiber in self._fibers.values()):
                raise PluginActivationError(
                    "disposed host cannot be activated",
                    plugin_id=None,
                    instance_id=None,
                    phase="activate",
                    source=str(self._config_path),
                )
            configs = validate_deployment(
                {
                    instance_id: self._definitions[self._plugin_ids[instance_id]]
                    for instance_id in self._instance_plan.order
                },
                self._config_path,
            )
            capabilities = {
                instance_id: self._capability_policy.authorize(
                    instance_id,
                    self._definitions[self._plugin_ids[instance_id]],
                )
                for instance_id in self._instance_plan.order
            }
            self._configs = configs
            self._capabilities = capabilities
            activated: list[Fiber] = []
            try:
                for instance_id in self._instance_plan.order:
                    fiber = self._fibers[instance_id]
                    if fiber.state is FiberState.ACTIVE:
                        continue
                    if fiber.state is FiberState.FAILED:
                        raise PluginActivationError(
                            "failed instance requires an explicit retry",
                            plugin_id=fiber.plugin_id,
                            instance_id=fiber.instance_id,
                            phase="activate",
                            source=fiber.source,
                        )
                    await self._activate_fiber(fiber)
                    activated.append(fiber)
            except asyncio.CancelledError:
                # 当前 Fiber 已在 _activate_fiber 收口。这里只回滚本次新激活的前置插件。
                await self._rollback(activated)
                raise
            except Exception as exc:
                extra = await self._rollback(activated)
                if isinstance(exc, (PluginLoadError, PluginActivationError)):
                    self._append_rollback(exc, extra)
                raise

    async def stop(self) -> None:
        """逆序停止活动实例并清理资源；成功停止的实例可再次激活。"""
        async with self._lock:
            await self._stop_in_reverse()

    async def dispose(self) -> None:
        """停止实例并进入终态；清理失败仍进入终态并抛出清理错误。"""
        async with self._lock:
            if all(fiber.state is FiberState.DISPOSED for fiber in self._fibers.values()):
                return
            try:
                await self._stop_in_reverse()
            finally:
                # 清理失败也不能挡住终态，否则第二次 dispose 还会再调钩子。
                self._mark_disposed()

    async def _activate_fiber(self, fiber: Fiber) -> None:
        definition = self._definitions[fiber.plugin_id]
        configs = self._configs
        capabilities = self._capabilities
        if configs is None or capabilities is None:
            raise PluginActivationError(
                "configuration or capabilities were not validated",
                plugin_id=fiber.plugin_id,
                instance_id=fiber.instance_id,
                phase="activate",
                source=fiber.source,
            )
        config = configs[fiber.instance_id]
        fiber.state = FiberState.LOADING
        fiber.setup_completed = False
        scope = ActivationScope(fiber.plugin_id, fiber.instance_id)
        fiber.scope = scope
        try:
            instance = load_plugin(definition, fiber.instance_id)
            fiber.instance = instance
            context = PluginContext(
                fiber.plugin_id,
                fiber.instance_id,
                config,
                scope,
                self._services.open_setup(
                    fiber.plugin_id,
                    fiber.instance_id,
                    fiber.source,
                    scope,
                ),
                capabilities[fiber.instance_id],
                self._events.open_activation(
                    fiber.plugin_id,
                    fiber.instance_id,
                    fiber.source,
                    scope,
                    capabilities[fiber.instance_id],
                ),
            )
            try:
                setup_result = await instance.setup(context, config)
                if setup_result is not None:
                    raise PluginActivationError(
                        "setup must return None",
                        plugin_id=fiber.plugin_id,
                        instance_id=fiber.instance_id,
                        phase="setup",
                        source=fiber.source,
                    )
            finally:
                # start 及其后的失败路径都不能再发布或取得服务。
                self._services.close_setup(fiber.instance_id)
            fiber.setup_completed = True
            start_result = await instance.start()
            if start_result is not None:
                raise PluginActivationError(
                    "start must return None",
                    plugin_id=fiber.plugin_id,
                    instance_id=fiber.instance_id,
                    phase="start",
                    source=fiber.source,
                )
            self._services.commit(fiber.plugin_id, fiber.instance_id, fiber.source)
            self._events.commit(fiber.instance_id)
        except asyncio.CancelledError as exc:
            # 取消继续向上冒泡，但已登记的资源先按这个取消异常收口。
            await self._fail(fiber, exc)
            raise
        except Exception as exc:
            # _fail 会清掉 setup_completed，阶段必须先记下来。
            completed = fiber.setup_completed
            local_errors = await self._fail(fiber, exc)
            if isinstance(exc, (PluginLoadError, PluginActivationError)):
                self._append_rollback(exc, local_errors)
                raise
            wrapped = PluginActivationError(
                "plugin activation failed",
                plugin_id=fiber.plugin_id,
                instance_id=fiber.instance_id,
                phase="start" if completed else "setup",
                source=fiber.source,
                cause=exc,
            )
            self._append_rollback(wrapped, local_errors)
            raise wrapped from exc
        fiber.state = FiberState.ACTIVE

    async def _fail(
        self,
        fiber: Fiber,
        cause: BaseException | None = None,
    ) -> tuple[BaseException, ...]:
        """关闭当前实例。stop 只在 setup 已成功时调用，清理错误全部返回。"""
        errors: tuple[BaseException, ...] = ()
        try:
            if fiber.setup_completed:
                errors, _cancel = await self._stop_fiber(fiber, FiberState.FAILED, cause)
            else:
                await self._drain_events(fiber.instance_id)
                errors, _cancel = await self._close_scope(fiber, cause)
        finally:
            leftover = fiber.scope
            fiber.instance = None
            fiber.scope = None
            fiber.setup_completed = False
            fiber.state = FiberState.FAILED
            try:
                if leftover is not None:
                    extra, _cancel = await leftover._finish(cause)
                    errors = errors + extra
            finally:
                # setup 未完成时没有走过 _stop_fiber。Scope 清理后再撤销 generation。
                # open_setup 之前的加载失败会在这里安全空返回。
                self._events.revoke(fiber.instance_id)
                self._services.revoke(fiber.instance_id)
        return errors

    async def _rollback(self, activated: list[Fiber]) -> tuple[BaseException, ...]:
        collected: list[BaseException] = []
        for fiber in reversed(activated):
            if fiber.state is not FiberState.ACTIVE:
                continue
            errors, _cancel = await self._stop_fiber(fiber, FiberState.PENDING)
            collected.extend(errors)
        return tuple(collected)

    def _append_rollback(
        self,
        error: PluginLoadError | PluginActivationError,
        extra: tuple[BaseException, ...],
    ) -> None:
        if extra:
            error.rollback_errors = error.rollback_errors + extra

    def _mark_disposed(self) -> None:
        for fiber in self._fibers.values():
            fiber.instance = None
            fiber.scope = None
            fiber.setup_completed = False
            fiber.state = FiberState.DISPOSED

    async def _stop_in_reverse(self) -> None:
        collected: list[BaseException] = []
        cancel: asyncio.CancelledError | None = None
        for instance_id in reversed(self._instance_plan.order):
            fiber = self._fibers[instance_id]
            if fiber.state is not FiberState.ACTIVE:
                continue
            errors, fiber_cancel = await self._stop_fiber(fiber, FiberState.PENDING)
            collected.extend(errors)
            if cancel is None:
                cancel = fiber_cancel
        if collected:
            raise PluginCleanupError(
                "plugin cleanup failed",
                plugin_id=None,
                instance_id=None,
                phase="stop",
                source=str(self._config_path),
                errors=tuple(collected),
            )
        if cancel is not None:
            raise cancel

    async def _stop_fiber(
        self,
        fiber: Fiber,
        success_state: FiberState,
        cause: BaseException | None = None,
    ) -> tuple[tuple[BaseException, ...], asyncio.CancelledError | None]:
        fiber.state = FiberState.UNLOADING
        errors: list[BaseException] = []
        cancel: asyncio.CancelledError | None = None
        try:
            # 在等待任何一类在途工作前，先同步关闭事件和服务的新入口。
            self._events.begin_drain(fiber.instance_id)
            self._services.begin_drain(fiber.instance_id)
            cancel = await self._drain_events(fiber.instance_id)
            # 在途调用结束时只唤醒自己的 Future，不能再来获取这把生命周期锁。
            await self._services.drain(fiber.instance_id)
            if fiber.setup_completed and fiber.instance is not None:
                try:
                    result = await fiber.instance.stop()
                    if result is not None:
                        errors.append(
                            PluginActivationError(
                                "stop must return None",
                                plugin_id=fiber.plugin_id,
                                instance_id=fiber.instance_id,
                                phase="stop",
                                source=fiber.source,
                            )
                        )
                except Exception as exc:  # noqa: BLE001 - 停止失败仍继续清理
                    errors.append(exc)
            closed, scope_cancel = await self._close_scope(fiber, cause)
            errors.extend(closed)
            if cancel is None:
                cancel = scope_cancel
        finally:
            self._events.revoke(fiber.instance_id)
            self._services.revoke(fiber.instance_id)
            leftover = fiber.scope
            fiber.instance = None
            fiber.scope = None
            fiber.setup_completed = False
            if leftover is not None:
                extra, leftover_cancel = await leftover._finish(cause)
                errors.extend(extra)
                if cancel is None:
                    cancel = leftover_cancel
            if errors:
                fiber.state = FiberState.FAILED
            else:
                fiber.state = success_state
        return tuple(errors), cancel

    async def _drain_events(self, instance_id: str) -> asyncio.CancelledError | None:
        """取消等待不丢弃已开始的回调；排空后由宿主继续清理并传播取消。"""
        cancelled: asyncio.CancelledError | None = None
        while True:
            try:
                await self._events.drain(instance_id)
            except asyncio.CancelledError as exc:
                if cancelled is None:
                    cancelled = exc
            else:
                return cancelled

    async def _close_scope(
        self,
        fiber: Fiber,
        cause: BaseException | None = None,
    ) -> tuple[tuple[BaseException, ...], asyncio.CancelledError | None]:
        if fiber.scope is None:
            return (), None
        result = await fiber.scope._finish(cause)
        fiber.scope = None
        return result
