"""服务暂存、提交、Handle，以及 generation 的租约和排空。"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import weakref
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Generic, NoReturn, Protocol, TypeVar, cast

from typing_extensions import TypeForm

from soul_plugin_core.contracts import PluginDefinition
from soul_plugin_core.errors import PluginActivationError, ServiceUnavailableError
from soul_plugin_core.instances import InstanceActivationPlan

T = TypeVar("T")
_REVOKED_IMPLEMENTATION = object()


@dataclass
class _CallFrame:
    """共享活动标记，避免子任务继承的已结束调用上下文产生误报。"""

    registry: weakref.ReferenceType[ServiceRegistry]
    provider_id: str
    active: bool = True


_call_frames: contextvars.ContextVar[tuple[_CallFrame, ...]] = contextvars.ContextVar(
    "soul_plugin_core_service_calls", default=()
)


class _Scope(Protocol):
    def defer(self, cleanup: Callable[[], object], label: str | None = None) -> object: ...


@dataclass
class _Registration:
    name: str
    implementation: object
    version: str
    provider_plugin_id: str
    provider_instance_id: str
    generation: int


class _Generation:
    """一次 Provider 激活的调用门。完成通知只唤醒当时创建的 Future。"""

    def __init__(self, generation: int) -> None:
        self.generation = generation
        self.accepting = True
        self.revoked = False
        self.inflight = 0
        self._waiters: list[asyncio.Future[None]] = []

    def notify(self) -> None:
        waiters = self._waiters
        self._waiters = []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)


class _Lease:
    """Consumer 持有的一张调用许可。释放它不会取消已经进入实现的调用。"""

    def __init__(self, registration: _Registration, state: _Generation) -> None:
        self.registration = registration
        self.state = state
        self.released = False


class _CallGate:
    def __init__(self, registry: ServiceRegistry, lease: _Lease) -> None:
        self._registry = registry
        self._lease = lease

    def enter(self) -> tuple[contextvars.Token[tuple[_CallFrame, ...]], _CallFrame]:
        self._registry.enter_call(self._lease)
        frame = _CallFrame(
            weakref.ref(self._registry), self._lease.registration.provider_instance_id
        )
        return _call_frames.set((*_call_frames.get(), frame)), frame

    def leave(self, token: contextvars.Token[tuple[_CallFrame, ...]], frame: _CallFrame) -> None:
        frame.active = False
        _call_frames.reset(token)
        self._registry.leave_call(self._lease)


class _ServiceProxy:
    """只转发 interface 声明的异步方法。实现对象不作为代理属性暴露。"""

    __slots__ = ("_gate", "_interface", "_registration")

    def __init__(self, registration: _Registration, interface: object, gate: _CallGate) -> None:
        object.__setattr__(self, "_registration", registration)
        object.__setattr__(self, "_interface", interface)
        object.__setattr__(self, "_gate", gate)

    def __getattribute__(self, name: str) -> object:
        # 槽位里的实现对象和调用门不能被读出来；其余缺失属性交给 __getattr__。
        if name in {"_gate", "_registration", "_interface"}:
            raise AttributeError(name)
        return object.__getattribute__(self, name)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(name)

    def __delattr__(self, name: str) -> None:
        raise AttributeError(name)

    def __getattr__(self, name: str) -> Callable[..., Awaitable[object]]:
        interface = object.__getattribute__(self, "_interface")
        registration = cast(_Registration, object.__getattribute__(self, "_registration"))
        implementation = registration.implementation
        declared = inspect.getattr_static(interface, name, None)
        if not inspect.iscoroutinefunction(declared):
            raise AttributeError(name)
        # 先看静态成员。property 和自定义 __getattr__ 都不能在这里被执行。
        if implementation is not _REVOKED_IMPLEMENTATION:
            static_target = inspect.getattr_static(implementation, name, None)
            if not inspect.iscoroutinefunction(static_target):
                raise AttributeError(name)
        gate = cast(_CallGate, object.__getattribute__(self, "_gate"))

        async def invoke(*args: object, **kwargs: object) -> object:
            token, frame = gate.enter()
            try:
                # 只在调用进入后绑定方法；缓存的代理方法不持有旧实现。
                current = registration.implementation
                static_method = inspect.getattr_static(current, name, None)
                if not inspect.iscoroutinefunction(static_method):
                    raise AttributeError(name)
                method = getattr(current, name)
                if not inspect.iscoroutinefunction(method):
                    raise AttributeError(name)
                return await method(*args, **kwargs)
            finally:
                gate.leave(token, frame)

        return invoke


class ServiceHandle(Generic[T]):
    """绑定到某个 Provider 代次的只读句柄。

    service 代理 interface 声明的异步方法；调用前检查租约，业务异常原样传递。
    """

    def __init__(
        self,
        registration: _Registration,
        interface: TypeForm[T],
        gate: _CallGate,
    ) -> None:
        self._registration = registration
        self._service = cast(T, _ServiceProxy(registration, interface, gate))

    @property
    def name(self) -> str:
        return self._registration.name

    @property
    def version(self) -> str:
        return self._registration.version

    @property
    def provider_plugin_id(self) -> str:
        return self._registration.provider_plugin_id

    @property
    def provider_instance_id(self) -> str:
        return self._registration.provider_instance_id

    @property
    def generation(self) -> int:
        return self._registration.generation

    @property
    def service(self) -> T:
        """返回有类型的异步代理；失效租约调用时抛 ServiceUnavailableError。"""
        return self._service


class ServiceRegistry:
    """一次 Host 上的服务登记。暂存项在提交前对其他实例不可见。"""

    def __init__(
        self,
        definitions: Mapping[str, PluginDefinition],
        plan: InstanceActivationPlan,
    ) -> None:
        self._definitions = definitions
        self._bindings = {
            (item.consumer_instance_id, item.target): item.provider_instance_id
            for item in plan.bindings
            if item.kind == "service"
        }
        self._open: set[str] = set()
        self._generations: dict[str, int] = {}
        self._current: dict[str, _Generation] = {}
        self._states: dict[tuple[str, int], _Generation] = {}
        self._consumer_leases: dict[str, list[_Lease]] = {}
        self._staged: dict[str, dict[str, object]] = {}
        self._available: dict[tuple[str, str], _Registration] = {}

    def open_setup(
        self,
        plugin_id: str,
        instance_id: str,
        source: str,
        scope: _Scope,
    ) -> ServiceFacade:
        """进入 setup。generation 在离开 PENDING 后的每次新激活上递增。"""
        for lease in self._consumer_leases.pop(instance_id, []):
            lease.released = True
        generation = self._generations.get(instance_id, 0) + 1
        self._generations[instance_id] = generation
        state = _Generation(generation)
        self._current[instance_id] = state
        self._states[(instance_id, generation)] = state
        self._staged[instance_id] = {}
        self._open.add(instance_id)
        scope.defer(self._discard_uncommitted(instance_id))
        scope.defer(self._release_leases(instance_id))
        return ServiceFacade(self, plugin_id, instance_id, source)

    def close_setup(self, instance_id: str) -> None:
        self._open.discard(instance_id)

    def commit(self, plugin_id: str, instance_id: str, source: str) -> None:
        """start 已返回。缺任何一个声明服务就整次提交失败，已暂存的内容留待 Scope 丢弃。"""
        definition = self._definitions[plugin_id]
        staged = self._staged.get(instance_id, {})
        missing = tuple(
            item.name for item in definition.provided_services if item.name not in staged
        )
        if missing:
            raise PluginActivationError(
                "declared service was not provided: " + ", ".join(missing),
                plugin_id=plugin_id,
                instance_id=instance_id,
                phase="start",
                source=source,
            )
        generation = self._generations[instance_id]
        published = {
            (instance_id, item.name): _Registration(
                name=item.name,
                implementation=staged[item.name],
                version=item.version,
                provider_plugin_id=plugin_id,
                provider_instance_id=instance_id,
                generation=generation,
            )
            for item in definition.provided_services
        }
        self._available.update(published)
        self._staged.pop(instance_id, None)

    def begin_drain(self, instance_id: str) -> None:
        """同步关闭新调用入口，以便与其他资源一起进入排空。"""
        state = self._current.get(instance_id)
        if state is None or state.revoked:
            return
        state.accepting = False

    async def drain(self, instance_id: str) -> None:
        """停止接收新调用，并等待这一 generation 的在途调用结束。"""
        self.begin_drain(instance_id)
        state = self._current.get(instance_id)
        if state is None or state.revoked:
            return
        while state.inflight > 0:
            waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            state._waiters.append(waiter)
            if state.inflight == 0:
                break
            await waiter

    def revoke(self, instance_id: str) -> None:
        """永久作废当前 generation。旧 Handle 仍持有状态对象，登记表不再保留它。"""
        state = self._current.get(instance_id)
        if state is None:
            return
        state.accepting = False
        state.revoked = True
        stale = [
            key
            for key, registration in self._available.items()
            if key[0] == instance_id and registration.generation == state.generation
        ]
        for key in stale:
            registration = self._available.pop(key)
            # 已排空的 generation 仅保留身份信息，不再持有服务实现。
            registration.implementation = _REVOKED_IMPLEMENTATION
        self._states.pop((instance_id, state.generation), None)
        if self._current.get(instance_id) is state:
            del self._current[instance_id]

    def accepts(self, registration: _Registration) -> bool:
        state = self._states.get((registration.provider_instance_id, registration.generation))
        return state is not None and state.accepting and not state.revoked

    def active_provider_ids(self) -> frozenset[str]:
        """当前调用链中仍在执行的本 Registry Provider，供宿主检测停用重入。"""
        return frozenset(
            frame.provider_id
            for frame in _call_frames.get()
            if frame.active and frame.registry() is self
        )

    def bind(self, consumer_id: str, registration: _Registration) -> _Lease:
        state = self._states[(registration.provider_instance_id, registration.generation)]
        lease = _Lease(registration, state)
        self._consumer_leases.setdefault(consumer_id, []).append(lease)
        return lease

    def enter_call(self, lease: _Lease) -> None:
        state = lease.state
        if lease.released or not state.accepting or state.revoked:
            registration = lease.registration
            raise ServiceUnavailableError(
                "service is unavailable",
                name=registration.name,
                provider_plugin_id=registration.provider_plugin_id,
                provider_instance_id=registration.provider_instance_id,
                generation=registration.generation,
            )
        state.inflight += 1

    def leave_call(self, lease: _Lease) -> None:
        state = lease.state
        state.inflight -= 1
        if state.inflight == 0:
            state.notify()

    def _discard_uncommitted(self, instance_id: str) -> Callable[[], None]:
        def discard() -> None:
            self._staged.pop(instance_id, None)

        return discard

    def _release_leases(self, instance_id: str) -> Callable[[], None]:
        def release() -> None:
            for lease in self._consumer_leases.pop(instance_id, []):
                lease.released = True

        return release


class ServiceFacade:
    """插件在 setup 期间能看见的三个方法。"""

    def __init__(
        self,
        registry: ServiceRegistry,
        plugin_id: str,
        instance_id: str,
        source: str,
    ) -> None:
        definition = registry._definitions[plugin_id]
        self._registry = registry
        self._plugin_id = plugin_id
        self._instance_id = instance_id
        self._source = source
        self._provided = {item.name for item in definition.provided_services}
        self._dependencies = {item.name: item for item in definition.service_dependencies}

    def provide(self, name: str, implementation: object) -> None:
        """仅在 setup 中暂存已声明服务；start 成功后由 Host 提交。"""
        self._ensure_setup()
        if name not in self._provided:
            self._fail("service is not declared by this plugin")
        staged = self._registry._staged[self._instance_id]
        if name in staged:
            self._fail("service was already provided in this activation")
        staged[name] = implementation

    def require(self, name: str, interface: TypeForm[T]) -> ServiceHandle[T]:
        """仅在 setup 中取得已绑定的强服务依赖。"""
        self._ensure_setup()
        dependency = self._dependencies.get(name)
        if dependency is None or dependency.optional:
            self._fail("require() only accepts a strong service dependency")
        return self._handle(self._require_committed(name), interface)

    def optional(self, name: str, interface: TypeForm[T]) -> ServiceHandle[T] | None:
        """仅在 setup 中取得可选服务；计划未绑定时返回 None。"""
        self._ensure_setup()
        dependency = self._dependencies.get(name)
        if dependency is None or not dependency.optional:
            self._fail("optional() only accepts an optional service dependency")
        if self._provider_instance_id(name) is None:
            return None
        return self._handle(self._require_committed(name), interface)

    def _ensure_setup(self) -> None:
        if self._instance_id not in self._registry._open:
            self._fail("provide, require, and optional can only be called from setup")

    def _provider_instance_id(self, name: str) -> str | None:
        return self._registry._bindings.get((self._instance_id, name))

    def _require_committed(self, name: str) -> _Registration:
        provider_instance_id = self._provider_instance_id(name)
        if provider_instance_id is None:
            self._fail("service binding is missing")
        registration = self._registry._available.get((provider_instance_id, name))
        if registration is None or not self._registry.accepts(registration):
            self._fail("service is not committed")
        return registration

    def _handle(self, registration: _Registration, interface: TypeForm[T]) -> ServiceHandle[T]:
        lease = self._registry.bind(self._instance_id, registration)
        return ServiceHandle(registration, interface, _CallGate(self._registry, lease))

    def _fail(self, message: str) -> NoReturn:
        raise PluginActivationError(
            message,
            plugin_id=self._plugin_id,
            instance_id=self._instance_id,
            phase="services",
            source=self._source,
        )
