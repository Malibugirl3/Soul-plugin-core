"""宿主：按实例计划激活，并在 start 成功后提交该实例暂存的服务。"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path

from soul_plugin_core.capabilities import CapabilityPolicy
from soul_plugin_core.config import validate_deployment
from soul_plugin_core.context import ActivationScope, PluginContext, active_effect_scopes
from soul_plugin_core.contracts import PluginDefinition
from soul_plugin_core.dependency import ActivationPlan
from soul_plugin_core.errors import PluginActivationError, PluginCleanupError, PluginLoadError
from soul_plugin_core.events import EventRegistry
from soul_plugin_core.extensions import ExtensionRegistry
from soul_plugin_core.instances import (
    InstanceActivationPlan,
    InstanceDefinition,
    expand_instance_plan,
)
from soul_plugin_core.lifecycle import Fiber, FiberState, load_plugin
from soul_plugin_core.services import ServiceRegistry

_StopOutcome = tuple[tuple[BaseException, ...], asyncio.CancelledError | None]


@dataclass(frozen=True)
class InstanceSnapshot:
    """一次宿主诊断时读取的实例状态；不含配置或插件对象。

    binding_blockers 表示当前不可用的已绑定 Provider；活动的 Optional
    消费者也可能出现该项，并不表示它已被停止。
    """

    instance_id: str
    plugin_id: str
    state: FiberState
    disabled: bool
    blockers: tuple[str, ...]
    binding_blockers: tuple[str, ...]


@dataclass
class _LifecycleFrame:
    """A shared live marker prevents stale inherited task contexts from blocking later work."""

    owner: object
    active: bool = True


_lifecycle_frames: ContextVar[tuple[_LifecycleFrame, ...]] = ContextVar(
    "soul_plugin_core_lifecycle_frames", default=()
)


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
        # 实际绑定是固定的：强依赖决定停用级联，全部绑定决定重新启动的前置条件。
        self._required_consumers: dict[str, set[str]] = {
            instance_id: set() for instance_id in self._instance_plan.order
        }
        self._required_providers: dict[str, set[str]] = {
            instance_id: set() for instance_id in self._instance_plan.order
        }
        self._bound_providers: dict[str, set[str]] = {
            instance_id: set() for instance_id in self._instance_plan.order
        }
        for binding in self._instance_plan.bindings:
            consumer = binding.consumer_instance_id
            provider = binding.provider_instance_id
            self._bound_providers[consumer].add(provider)
            definition = by_id[self._plugin_ids[consumer]]
            if binding.kind == "plugin":
                optional = next(
                    dep.optional
                    for dep in definition.plugin_dependencies
                    if dep.plugin_id == binding.target
                )
            else:
                optional = next(
                    dep.optional
                    for dep in definition.service_dependencies
                    if dep.name == binding.target
                )
            if not optional:
                self._required_consumers[provider].add(consumer)
                self._required_providers[consumer].add(provider)
        self._disabled: set[str] = set()
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
        self._extensions = ExtensionRegistry()
        self._lock = asyncio.Lock()
        self._lifecycle_owner = object()

    @asynccontextmanager
    async def _operation(self) -> AsyncIterator[None]:
        """Reject lifecycle reentry before acquiring the non-reentrant lock."""
        if any(
            frame.owner is self._lifecycle_owner and frame.active
            for frame in _lifecycle_frames.get()
        ):
            self._lifecycle_error("lifecycle operations cannot be awaited from lifecycle hooks")
        # A concurrent shutdown may already hold the lock while waiting for this
        # callback. Waiting for that lock inside the callback would deadlock.
        if self._lock.locked() and self._active_callback_owners():
            self._lifecycle_error(
                "lifecycle operation cannot wait for a host draining this callback"
            )
        async with self._lock:
            frame = _LifecycleFrame(self._lifecycle_owner)
            token = _lifecycle_frames.set((*_lifecycle_frames.get(), frame))
            try:
                yield
            finally:
                frame.active = False
                _lifecycle_frames.reset(token)

    def _lifecycle_error(self, message: str, instance_id: str | None = None) -> None:
        fiber = self._fibers.get(instance_id) if instance_id is not None else None
        raise PluginActivationError(
            message,
            plugin_id=fiber.plugin_id if fiber is not None else None,
            instance_id=instance_id,
            phase="lifecycle",
            source=fiber.source if fiber is not None else str(self._config_path),
        )

    def _ensure_no_self_drain(self, selected: set[str]) -> None:
        conflicts = self._active_callback_owners().intersection(selected)
        if conflicts:
            instance_id = min(conflicts)
            self._lifecycle_error(
                "cannot stop an instance from its active service call or event callback or effect",
                instance_id,
            )

    def _active_callback_owners(self) -> frozenset[str]:
        roots: set[ActivationScope] = set()
        for scope in active_effect_scopes():
            while scope._parent is not None:
                scope = scope._parent
            roots.add(scope)
        effect_owners = (
            {instance_id for instance_id, fiber in self._fibers.items() if fiber.scope in roots}
            if roots
            else set()
        )
        return (
            self._services.active_provider_ids()
            | self._events.active_instance_ids()
            | self._extensions.active_instance_ids()
            | effect_owners
        )

    @property
    def instance_plan(self) -> InstanceActivationPlan:
        """返回不可变的实例级部署计划，不读取配置或导入插件。"""
        return self._instance_plan

    def snapshot(self) -> tuple[InstanceSnapshot, ...]:
        """按计划顺序返回瞬时状态；同一事件循环内同步采集，不提供跨线程保证。"""
        return tuple(
            InstanceSnapshot(
                instance_id=instance_id,
                plugin_id=self._plugin_ids[instance_id],
                state=self._fibers[instance_id].state,
                disabled=instance_id in self._disabled,
                blockers=self.get_blockers(instance_id),
                binding_blockers=self.get_binding_blockers(instance_id),
            )
            for instance_id in self._instance_plan.order
        )

    def get_state(self, instance_id: str) -> FiberState:
        """返回实例的当前状态；未知实例由映射抛出 KeyError。"""
        return self._fibers[instance_id].state

    def is_disabled(self, instance_id: str) -> bool:
        """实例是否被显式停用；依赖暂不可用不等同于显式停用。"""
        self._fibers[instance_id]
        return instance_id in self._disabled

    def get_blockers(self, instance_id: str) -> tuple[str, ...]:
        """返回当前未 ACTIVE 的直接强依赖实例，按实例 ID 排序。"""
        return tuple(
            sorted(
                provider
                for provider in self._required_providers[instance_id]
                if self._fibers[provider].state is not FiberState.ACTIVE
            )
        )

    def get_binding_blockers(self, instance_id: str) -> tuple[str, ...]:
        """返回未 ACTIVE 的已绑定 Provider，包括已绑定的可选依赖。

        未绑定的可选依赖不会阻止启动；已绑定的可选依赖需要 Provider
        恢复后才能重新 setup，现存活动消费者的旧句柄不会自动重绑。
        """
        return self._unavailable_bindings(instance_id)

    async def deactivate(self, instance_id: str) -> None:
        """停用实例并逆序停止其强依赖消费者；保留实例声明和绑定。

        已绑定的可选消费者保持运行，其旧句柄失效且不会自动重绑。
        此操作不注销实例，也不检测外部服务故障。
        """
        self._fibers[instance_id]  # 未知实例不改变宿主状态。
        affected = {instance_id}
        pending = [instance_id]
        while pending:
            for consumer in self._required_consumers[pending.pop()]:
                if consumer not in affected:
                    affected.add(consumer)
                    pending.append(consumer)
        self._ensure_no_self_drain(affected)
        async with self._operation():
            self._ensure_not_disposed()
            self._disabled.add(instance_id)
            await self._stop_in_reverse(affected)

    async def activate_instance(self, instance_id: str) -> None:
        """恢复一个实例；其已绑定 Provider 必须已经 ACTIVE。

        不自动恢复消费者；可随后调用 activate() 按原计划启动可用实例。
        失败时保留显式停用意图，FAILED 实例不会被自动重试。
        """
        async with self._operation():
            fiber = self._fibers[instance_id]
            self._ensure_not_disposed()
            if fiber.state is FiberState.FAILED:
                self._raise_failed(fiber)
            if fiber.state is FiberState.ACTIVE:
                self._disabled.discard(instance_id)
                return
            unavailable = self._unavailable_bindings(instance_id)
            if unavailable:
                raise PluginActivationError(
                    "bound provider is not active: " + ", ".join(unavailable),
                    plugin_id=fiber.plugin_id,
                    instance_id=instance_id,
                    phase="activate",
                    source=fiber.source,
                )
            self._preflight()
            await self._activate_fiber(fiber)
            self._disabled.discard(instance_id)

    async def activate(self) -> None:
        """按依赖顺序激活；失败或取消时逆序回滚本次新激活的实例。"""
        async with self._operation():
            self._ensure_not_disposed()
            self._preflight()
            activated: list[Fiber] = []
            try:
                for instance_id in self._instance_plan.order:
                    fiber = self._fibers[instance_id]
                    if fiber.state is FiberState.ACTIVE or instance_id in self._disabled:
                        continue
                    if fiber.state is FiberState.FAILED:
                        self._raise_failed(fiber)
                    if self._unavailable_bindings(instance_id):
                        continue
                    await self._activate_fiber(fiber)
                    activated.append(fiber)
            except asyncio.CancelledError:
                await self._rollback(activated)
                raise
            except Exception as exc:
                extra = await self._rollback(activated)
                if isinstance(exc, (PluginLoadError, PluginActivationError)):
                    self._append_rollback(exc, extra)
                raise

    def _ensure_not_disposed(self) -> None:
        if any(fiber.state is FiberState.DISPOSED for fiber in self._fibers.values()):
            raise PluginActivationError(
                "disposed host cannot be activated",
                plugin_id=None,
                instance_id=None,
                phase="activate",
                source=str(self._config_path),
            )

    @staticmethod
    def _raise_failed(fiber: Fiber) -> None:
        raise PluginActivationError(
            "failed instance requires an explicit retry",
            plugin_id=fiber.plugin_id,
            instance_id=fiber.instance_id,
            phase="activate",
            source=fiber.source,
        )

    def _unavailable_bindings(self, instance_id: str) -> tuple[str, ...]:
        return tuple(
            sorted(
                provider
                for provider in self._bound_providers[instance_id]
                if self._fibers[provider].state is not FiberState.ACTIVE
            )
        )

    def _preflight(self) -> None:
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

    async def stop(self) -> None:
        """逆序停止活动实例并清理资源；成功停止的实例可再次激活。"""
        self._ensure_no_self_drain(set(self._fibers))
        async with self._operation():
            await self._stop_in_reverse()

    async def dispose(self) -> None:
        """停止实例并进入终态；清理失败仍进入终态并抛出清理错误。"""
        self._ensure_no_self_drain(set(self._fibers))
        async with self._operation():
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
                self._extensions.open_activation(
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
            self._extensions.commit(fiber.instance_id)
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
                self._events.begin_drain(fiber.instance_id)
                self._extensions.begin_drain(fiber.instance_id)
                await self._drain_events(fiber.instance_id)
                await self._drain_extensions(fiber.instance_id)
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
                self._extensions.revoke(fiber.instance_id)
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

    async def _stop_in_reverse(self, selected: set[str] | None = None) -> None:
        collected: list[BaseException] = []
        cancel: asyncio.CancelledError | None = None
        for instance_id in reversed(self._instance_plan.order):
            if selected is not None and instance_id not in selected:
                continue
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
            self._extensions.begin_drain(fiber.instance_id)
            self._services.begin_drain(fiber.instance_id)
            cancel = await self._drain_events(fiber.instance_id)
            extension_cancel = await self._drain_extensions(fiber.instance_id)
            if cancel is None:
                cancel = extension_cancel
            # 在途调用结束时只唤醒自己的 Future，不能再来获取这把生命周期锁。
            service_cancel = await self._drain_services(fiber.instance_id)
            if cancel is None:
                cancel = service_cancel
            closed, stop_cancel = await self._stop_and_close(fiber, cause)
            errors.extend(closed)
            if cancel is None:
                cancel = stop_cancel
        finally:
            self._events.revoke(fiber.instance_id)
            self._extensions.revoke(fiber.instance_id)
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
        return await self._drain_with_cancellation(self._events.drain, instance_id)

    async def _drain_extensions(self, instance_id: str) -> asyncio.CancelledError | None:
        """等待已进入的观察回调，不让外部取消中断生命周期收口。"""
        return await self._drain_with_cancellation(self._extensions.drain, instance_id)

    async def _drain_services(self, instance_id: str) -> asyncio.CancelledError | None:
        """取消等待不提前撤销在途服务；调用结束后继续停止并传播取消。"""
        return await self._drain_with_cancellation(self._services.drain, instance_id)

    @staticmethod
    async def _drain_with_cancellation(
        drain: Callable[[str], Awaitable[None]], instance_id: str
    ) -> asyncio.CancelledError | None:
        cancelled: asyncio.CancelledError | None = None
        while True:
            try:
                await drain(instance_id)
            except asyncio.CancelledError as exc:
                if cancelled is None:
                    cancelled = exc
            else:
                return cancelled

    async def _stop_and_close(
        self, fiber: Fiber, cause: BaseException | None
    ) -> tuple[tuple[BaseException, ...], asyncio.CancelledError | None]:
        """调用方取消只中断等待；停止钩子和资源关闭仍在同一任务里收口。"""
        completed: list[_StopOutcome] = []
        pending = asyncio.create_task(self._run_stop_and_scope(fiber, cause, completed))
        cancelled: asyncio.CancelledError | None = None
        while True:
            try:
                errors, inner_cancel = await asyncio.shield(pending)
            except asyncio.CancelledError as exc:
                if pending.cancelled():
                    try:
                        pending.result()
                    except asyncio.CancelledError as stop_cancel:
                        # stop() 可能自行取消当前 Task 后同步返回；即便 Task
                        # 最终呈取消状态，同步清理已保存的错误也不能丢失。
                        errors = completed[0][0] if completed else ()
                        return errors, cancelled if cancelled is not None else stop_cancel
                if cancelled is None:
                    cancelled = exc
                if pending.done():
                    errors, _inner_cancel = pending.result()
                    return errors, cancelled
            else:
                return errors, cancelled if cancelled is not None else inner_cancel

    async def _run_stop_and_scope(
        self, fiber: Fiber, cause: BaseException | None, completed: list[_StopOutcome]
    ) -> _StopOutcome:
        """stop 返回后立即关闭 Scope，让同步租约释放不被额外调度延迟。"""
        errors: list[BaseException] = []
        cancelled: asyncio.CancelledError | None = None
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
            except asyncio.CancelledError as exc:
                cancelled = exc
            except Exception as exc:  # noqa: BLE001 - 停止失败仍继续清理
                errors.append(exc)
        closed, scope_cancel = await self._close_scope(fiber, cause)
        errors.extend(closed)
        if cancelled is None:
            cancelled = scope_cancel
        outcome = (tuple(errors), cancelled)
        completed.append(outcome)
        return outcome

    async def _close_scope(
        self,
        fiber: Fiber,
        cause: BaseException | None = None,
    ) -> tuple[tuple[BaseException, ...], asyncio.CancelledError | None]:
        scope = fiber.scope
        if scope is None:
            return (), None
        cancelled: asyncio.CancelledError | None = None
        while True:
            try:
                errors, scope_cancel = await scope._finish(cause)
            except asyncio.CancelledError as exc:
                if cancelled is None:
                    cancelled = exc
            else:
                fiber.scope = None
                return errors, cancelled if cancelled is not None else scope_cancel
