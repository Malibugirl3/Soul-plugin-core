"""插件上下文：身份、只读配置、可单独关闭的 defer，以及 setup 期间的服务门面。"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import sys
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum, auto
from inspect import isawaitable
from types import FunctionType, TracebackType
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar, cast
from weakref import ReferenceType, WeakKeyDictionary, WeakSet, ref

from soul_plugin_core.errors import PluginActivationError, PluginCleanupError
from soul_plugin_core.services import ServiceFacade

if TYPE_CHECKING:
    from soul_plugin_core.events import EventFacade
    from soul_plugin_core.extensions import ExtensionFacade

Cleanup = Callable[[], object]
T = TypeVar("T")
_T_co = TypeVar("_T_co", covariant=True)
_ExitArgs = tuple[type[BaseException] | None, BaseException | None, TracebackType | None]


@dataclass
class _TaskFrame:
    """A task's inherited ownership marker; neither scope nor task is kept alive."""

    scope: ReferenceType[ActivationScope]
    task: ReferenceType[asyncio.Task[object]] | None = None

    def live(self) -> bool:
        if self.task is None:
            # A custom eager task factory may execute before create_task returns.
            return True
        task = self.task()
        return task is not None and not task.done()


_task_frames: contextvars.ContextVar[tuple[_TaskFrame, ...]] = contextvars.ContextVar(
    "soul_plugin_core_managed_tasks", default=()
)


def active_managed_scopes() -> tuple[ActivationScope, ...]:
    """Return still-live managed task owners inherited by the current call chain."""
    scopes: list[ActivationScope] = []
    for frame in _task_frames.get():
        if frame.live() and (scope := frame.scope()) is not None:
            scopes.append(scope)
    return tuple(scopes)


@dataclass
class _CleanupFrame:
    scope: ReferenceType[ActivationScope]
    effect: ReferenceType[_Effect]
    active: bool = True


_cleanup_frames: contextvars.ContextVar[tuple[_CleanupFrame, ...]] = contextvars.ContextVar(
    "soul_plugin_core_cleanup_calls", default=()
)
_awaited_cleanup_tasks: WeakKeyDictionary[asyncio.Task[object], list[_CleanupFrame]] = (
    WeakKeyDictionary()
)


def _track_cleanup_task(result: object, frame: _CleanupFrame | None) -> None:
    if frame is not None and isinstance(result, asyncio.Task):
        tracked = cast(asyncio.Task[object], result)
        _awaited_cleanup_tasks.setdefault(tracked, []).append(frame)


def _untrack_cleanup_task(result: object, frame: _CleanupFrame | None) -> None:
    if frame is None or not isinstance(result, asyncio.Task):
        return
    tracked = cast(asyncio.Task[object], result)
    frames = _awaited_cleanup_tasks.get(tracked)
    if frames is None:
        return
    remaining = [item for item in frames if item is not frame]
    if remaining:
        _awaited_cleanup_tasks[tracked] = remaining
    else:
        _awaited_cleanup_tasks.pop(tracked, None)


def _active_cleanup_frames() -> tuple[_CleanupFrame, ...]:
    frames = _cleanup_frames.get()
    current = asyncio.current_task()
    if current is not None:
        frames += tuple(_awaited_cleanup_tasks.get(current, ()))
    return tuple(frame for frame in frames if frame.active)


def active_effect_scopes() -> tuple[ActivationScope, ...]:
    """Scope owners of live managed tasks and running cleanup callbacks."""
    scopes = list(active_managed_scopes())
    for frame in _active_cleanup_frames():
        if (scope := frame.scope()) is not None:
            scopes.append(scope)
    return tuple(scopes)


def _coroutine_has_task(
    loop: asyncio.AbstractEventLoop,
    coroutine: Coroutine[object, object, object],
) -> bool:
    """查找当前事件循环中拥有此协程的存活 Task。"""

    def public_check() -> bool:
        return any(task.get_coro() is coroutine for task in asyncio.all_tasks(loop))

    # 这两个版本的 CPython all_tasks() 正是从这个 WeakSet 构建集合。
    # 直接检查其快照可避免创建第二个集合，也无需过滤每个无关 Task。
    # 其他解释器、版本和替换过的 all_tasks() 一律使用公开 API。
    if sys.implementation.name != "cpython" or sys.version_info[:2] not in ((3, 10), (3, 11)):
        return public_check()
    registry = getattr(asyncio.tasks, "_all_tasks", None)
    all_tasks = asyncio.all_tasks
    if not isinstance(all_tasks, FunctionType):
        return public_check()
    if (
        not isinstance(registry, WeakSet)
        or all_tasks is not getattr(asyncio.tasks, "all_tasks", None)
        or all_tasks.__module__ != "asyncio.tasks"
        or all_tasks.__globals__.get("_all_tasks") is not registry
        or all_tasks.__code__.co_names
        != ("events", "get_running_loop", "list", "_all_tasks", "RuntimeError")
    ):
        return public_check()
    registry = cast(WeakSet[asyncio.Task[object]], registry)

    # 与 asyncio.all_tasks() 一样，其他线程可能同时更新 WeakSet；
    # 获取稳定快照前不直接遍历它，并对并发修改重试。
    for attempt in range(1000):
        try:
            tasks = list(registry)
            break
        except RuntimeError:
            if attempt == 999:
                raise
    else:  # pragma: no cover - 最后一次失败会直接抛出
        return public_check()

    found = False
    for task in tasks:
        if type(task) is not asyncio.Task:
            # 自定义 Task 可以覆盖 get_coro/get_loop；维持 all_tasks 的行为。
            return public_check()
        if task.get_coro() is coroutine and task.get_loop() is loop and not task.done():
            found = True
    return found


class EffectScopeState(Enum):
    """Scope 只前进，底层值不是稳定协议。"""

    OPEN = auto()
    CLOSING = auto()
    CLOSED = auto()


class _AsyncResource(Protocol[_T_co]):
    async def __aenter__(self) -> _T_co: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...


class _CloseResult:
    """一次关闭事务的不可变观察面。领取取消不会改错误 tuple。"""

    def __init__(
        self,
        errors: tuple[BaseException, ...],
        cancel: asyncio.CancelledError | None,
    ) -> None:
        self.errors = errors
        self._cancel = cancel

    def take_cancel(self) -> asyncio.CancelledError | None:
        cancel = self._cancel
        self._cancel = None
        return cancel


class _Flatten:
    """子 Scope 的错误要按原样展开，不包成另一层清理错误。"""

    def __init__(self, errors: tuple[BaseException, ...]) -> None:
        self.errors = errors


class _PropagatedCancel:
    """子事务结束时带出的取消。其余错误仍展开，取消本身不是清理错误。"""

    def __init__(
        self,
        error: asyncio.CancelledError,
        errors: tuple[BaseException, ...],
    ) -> None:
        self.error = error
        self.errors = errors


_Outcome = BaseException | None | _Flatten | _PropagatedCancel


class _DoneWaiter(Protocol):
    def done(self) -> bool: ...

    def cancelled(self) -> bool: ...

    def exception(self) -> BaseException | None: ...


def _stored_cancellation(waiter: _DoneWaiter | None) -> asyncio.CancelledError | None:
    """取出已结束等待对象上的取消。已结束和尚未结束两种时序都走这里。"""
    if waiter is None or not waiter.done():
        return None
    if waiter.cancelled():
        try:
            waiter.exception()
        except asyncio.CancelledError as exc:
            return exc
        return asyncio.CancelledError()
    error = waiter.exception()
    if isinstance(error, asyncio.CancelledError):
        return error
    return None


class _Pending:
    """下一项要等待。等待对象属于共享事务，不挂在某一个调用方上。"""

    def __init__(self, wait: Callable[[], Awaitable[_Outcome]]) -> None:
        self._wait = wait

    def wait(self) -> Awaitable[_Outcome]:
        return self._wait()


class _Effect:
    """一项 cleanup。Handle 关闭它时使用独立任务，不挂在调用方协程上。"""

    def __init__(self, cleanup: Cleanup, label: str | None, owner: ActivationScope) -> None:
        self.label = label
        self._cleanup = cleanup
        self._owner = ref(owner)
        self._task: asyncio.Task[None] | None = None
        self.finished = False
        self._error: BaseException | None = None
        self._reported = False

    def ensure_task(self) -> asyncio.Task[None]:
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())
        return self._task

    async def _run(self) -> None:
        with self._calling() as frame:
            result: object = None
            try:
                result = self._cleanup()
                if isawaitable(result):
                    _track_cleanup_task(result, frame)
                    await result
            except asyncio.CancelledError as exc:
                self._error = exc
            except Exception as exc:  # noqa: BLE001 - 先保存，等关闭方真正观察到再报告
                self._error = exc
            finally:
                _untrack_cleanup_task(result, frame)
                self.finished = True

    @contextmanager
    def _calling(self) -> Iterator[_CleanupFrame | None]:
        owner = self._owner()
        if owner is None:
            yield None
            return
        frame = _CleanupFrame(ref(owner), ref(self))
        token = _cleanup_frames.set((*_cleanup_frames.get(), frame))
        try:
            yield frame
        finally:
            frame.active = False
            _cleanup_frames.reset(token)

    def _claim(self) -> BaseException | None:
        """一次正常观察取走异常。调用方取消发生在进入这里之前，不会消费它。"""
        if self._reported:
            return None
        self._reported = True
        return self._error

    async def wait_for_handle(self) -> BaseException | None:
        """Handle 关闭使用独立任务。调用方被取消时，这项 cleanup 继续运行。"""
        if not self.finished:
            await asyncio.shield(self.ensure_task())
        return self._claim()

    def poll(self, _cause: BaseException | None) -> _Outcome | _Pending:
        """能同步做完就当场返回。需要等待时只启动一次，交给共享事务去等。"""
        if self._task is not None:
            if self._task.done():
                return self._after_task()
            return _Pending(self._join)
        if self.finished:
            return self._claim()
        owner = self._owner()
        frame = _CleanupFrame(ref(owner), ref(self)) if owner is not None else None
        token = _cleanup_frames.set((*_cleanup_frames.get(), frame)) if frame is not None else None
        try:
            result = self._cleanup()
        except asyncio.CancelledError as exc:
            if frame is not None:
                frame.active = False
            self._error = exc
            self.finished = True
            return self._claim()
        except Exception as exc:  # noqa: BLE001 - 汇总后继续其余项
            if frame is not None:
                frame.active = False
            self._error = exc
            self.finished = True
            return self._claim()
        finally:
            if token is not None:
                _cleanup_frames.reset(token)
        if isawaitable(result):
            _track_cleanup_task(result, frame)
            self._task = asyncio.get_running_loop().create_task(self._await_started(result, frame))
            return _Pending(self._join)
        if frame is not None:
            frame.active = False
        self.finished = True
        return self._claim()

    async def _await_started(
        self, result: Awaitable[object], frame: _CleanupFrame | None = None
    ) -> None:
        with self._calling():
            try:
                await result
            except asyncio.CancelledError as exc:
                self._error = exc
            except Exception as exc:  # noqa: BLE001 - 先保存，等关闭方真正观察到再报告
                self._error = exc
            finally:
                if frame is not None:
                    frame.active = False
                _untrack_cleanup_task(result, frame)
                self.finished = True

    async def _join(self) -> BaseException | None:
        task = self._task
        if task is None:
            return self._claim()
        if not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                if not task.done():
                    raise
        return self._after_task()

    def _after_task(self) -> BaseException | None:
        """无论检查时任务是否已经结束，都从同一份结果里领取。"""
        self.finished = True
        if self._error is None:
            cancelled = _stored_cancellation(self._task)
            if cancelled is not None:
                self._error = cancelled
        return self._claim()


class _ManagedTask:
    """受管任务。取消只发送一次；完成后保留待报告的异常并移除可释放项。"""

    def __init__(
        self, task: asyncio.Task[object], label: str | None, owner: ActivationScope
    ) -> None:
        self.label = label
        self._task = task
        self._error: BaseException | None = None
        self._reported = False
        self._cancel_sent = False
        self._finished = asyncio.Event()
        self._owner = ref(owner)
        task.add_done_callback(self._on_done)

    @property
    def task(self) -> asyncio.Task[object]:
        return self._task

    def _on_done(self, task: asyncio.Task[object]) -> None:
        self._capture(task)
        if self._error is None or self._reported:
            self._discard()
        self._finished.set()

    def _discard(self) -> None:
        owner = self._owner()
        if owner is not None:
            owner._discard_task(self)

    def _capture(self, task: asyncio.Task[object] | None = None) -> None:
        current = self._task if task is None else task
        if not current.done() or current.cancelled():
            return
        try:
            error = current.exception()
        except asyncio.CancelledError:
            return
        if error is not None:
            self._error = error

    def request_cancel(self) -> None:
        if self._cancel_sent:
            return
        self._cancel_sent = True
        if not self._task.done():
            self._task.cancel()

    def _claim(self) -> BaseException | None:
        if self._reported:
            return None
        self._capture()
        self._reported = True
        self._discard()
        if self._task.cancelled():
            return None
        return self._error

    async def wait_for_handle(self) -> BaseException | None:
        self.request_cancel()
        return await self._join()

    def poll(self, _cause: BaseException | None) -> _Outcome | _Pending:
        if self._task.done():
            return self._claim()
        return _Pending(self._join)

    async def _join(self) -> BaseException | None:
        if not self._task.done():
            # 等待完成通知而不是直接 await 任务：业务异常只通过 _claim 报告，
            # 取消等待者也不会取消任务，或误把任务结果视为已领取。
            await self._finished.wait()
        return self._claim()


class _ChildLink:
    """父栈上的子 Scope。关闭时进入子树自己的那一次事务。"""

    def __init__(self, child: ActivationScope) -> None:
        self.child = child

    def poll(self, cause: BaseException | None) -> _Outcome | _Pending:
        child = self.child
        if child._state is not EffectScopeState.CLOSED:
            if child._future is None:
                child._arm(cause)
                child._pump_sync()
            future = child._future
            if future is not None and not future.done():
                return _Pending(self._join)
        return self._child_result()

    async def _join(self) -> _Outcome:
        future = self.child._future
        if future is not None and not future.done():
            await asyncio.shield(future)
        return self._child_result()

    def _child_result(self) -> _Outcome:
        result = self.child._result
        cancel = result.take_cancel()
        if cancel is None:
            return _Flatten(result.errors)
        return _PropagatedCancel(cancel, result.errors)


class ActivationScope:
    """一次激活的清理栈。同一次关闭只有一个事务在遍历栈。"""

    def __init__(
        self,
        plugin_id: str | None = None,
        instance_id: str | None = None,
        *,
        label: str | None = None,
        parent: ActivationScope | None = None,
    ) -> None:
        self._plugin_id = plugin_id
        self._instance_id = instance_id
        self.label = label
        self._parent = parent
        # The ordered mapping is both a LIFO stack and an O(1) removal index for
        # completed tasks; retaining every successful task grows without bound.
        self._effects: dict[_Effect | _ChildLink | _ManagedTask, None] = {}
        self._current: _Effect | _ChildLink | _ManagedTask | None = None
        self._state = EffectScopeState.OPEN
        self._cause: BaseException | None = None
        self._errors_list: list[BaseException] = []
        self._cancel: asyncio.CancelledError | None = None
        self._result = _CloseResult((), None)
        self._future: asyncio.Future[_CloseResult] | None = None
        self._driver: asyncio.Task[None] | None = None
        self._task_cancellation_sent = False

    @property
    def state(self) -> EffectScopeState:
        return self._state

    def defer(self, cleanup: Cleanup, label: str | None = None) -> EffectHandle:
        self._reject_if_closed()
        effect = _Effect(cleanup, label, self)
        self._effects[effect] = None
        return EffectHandle(effect)

    async def enter_async_context(
        self,
        manager: _AsyncResource[T],
        *,
        label: str | None = None,
    ) -> T:
        """只在进入成功后登记退出。退出时读取本 Scope 关闭事务上的 cause。"""
        self._reject_if_closed()
        value = await manager.__aenter__()
        try:
            self.defer(lambda: manager.__aexit__(*self._exit_args()), label=label)
        except PluginActivationError:
            await manager.__aexit__(None, None, None)
            raise
        return value

    def child_scope(self, *, label: str | None = None) -> ActivationScope:
        self._reject_if_closed()
        child = ActivationScope(
            self._plugin_id,
            self._instance_id,
            label=label,
            parent=self,
        )
        self._effects[_ChildLink(child)] = None
        return child

    def create_task(
        self,
        coroutine: Coroutine[object, object, T],
        *,
        label: str | None = None,
    ) -> TaskHandle[T]:
        prepared: Coroutine[object, object, T] = self._prepare_coroutine(coroutine)
        frame = _TaskFrame(ref(self))
        token = _task_frames.set((*_task_frames.get(), frame))
        try:
            task = asyncio.get_running_loop().create_task(prepared)
        except Exception:
            # 创建失败后没有 Scope 接管这个新协程，避免产生未等待协程警告。
            prepared.close()
            raise
        finally:
            _task_frames.reset(token)
        frame.task = ref(cast(asyncio.Task[object], task))
        item = _ManagedTask(cast(asyncio.Task[object], task), label, self)
        self._effects[item] = None
        return TaskHandle(item)

    def _discard_task(self, item: _ManagedTask) -> None:
        self._effects.pop(item, None)

    def _prepare_coroutine(
        self,
        coroutine: Coroutine[object, object, T] | object,
    ) -> Coroutine[object, object, T]:
        if isinstance(coroutine, asyncio.Task):
            raise self._task_type_error()
        if not inspect.iscoroutine(coroutine):
            raise self._task_type_error()
        if inspect.getcoroutinestate(coroutine) != inspect.CORO_CREATED:
            raise self._task_type_error()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not None and _coroutine_has_task(loop, coroutine):
            # Task 刚创建、尚未运行时，其协程仍显示为 CORO_CREATED。
            # 不能把别人正在调度的协程接管或在拒绝时关闭。
            raise self._task_type_error()
        try:
            self._reject_if_closed()
        except PluginActivationError:
            coroutine.close()
            raise
        return coroutine

    def _task_type_error(self) -> PluginActivationError:
        return PluginActivationError(
            "create_task() requires a coroutine object",
            plugin_id=self._plugin_id,
            instance_id=self._instance_id,
            phase="effects",
            source="activation-scope",
        )

    async def aclose(self) -> None:
        errors = await self.close()
        if errors:
            raise PluginCleanupError(
                "plugin cleanup failed",
                plugin_id=self._plugin_id,
                instance_id=self._instance_id,
                phase="effects",
                source="activation-scope",
                errors=errors,
            )

    async def close(self, cause: BaseException | None = None) -> tuple[BaseException, ...]:
        """按后进先出清理。并发调用等待同一个事务，单项失败不阻止其余项。"""
        self._reject_self_join()
        errors, cancel = await self._finish(cause)
        if cancel is not None:
            raise cancel
        return errors

    def _reject_self_join(self) -> None:
        """A managed task cannot await closure of a scope which owns it."""
        for owner in active_effect_scopes():
            scope: ActivationScope | None = owner
            while scope is not None:
                if scope is self:
                    raise PluginActivationError(
                        "a managed task cannot close its owning scope",
                        plugin_id=self._plugin_id,
                        instance_id=self._instance_id,
                        phase="effects",
                        source="activation-scope",
                    )
                scope = scope._parent

    async def _finish(
        self,
        cause: BaseException | None = None,
    ) -> tuple[tuple[BaseException, ...], asyncio.CancelledError | None]:
        """Host 和内部事务读取关闭结果。领取取消，不把取消设到 Future 上。"""
        result = await self._wait_close(cause)
        return result.errors, result.take_cancel()

    async def _wait_close(self, cause: BaseException | None = None) -> _CloseResult:
        if self._state is EffectScopeState.CLOSED:
            return self._result
        if self._future is None:
            self._arm(cause)
            self._pump_sync()
        future = self._future
        if future is not None and not future.done():
            await asyncio.shield(future)
        return self._result

    def _arm(self, cause: BaseException | None) -> None:
        if self._future is not None or self._state is not EffectScopeState.OPEN:
            return
        self._state = EffectScopeState.CLOSING
        if cause is not None:
            self._cause = cause
        self._future = asyncio.get_running_loop().create_future()
        self._cancel_tree()

    def _cancel_tree(self) -> None:
        """关闭开始时给当时已存在的整棵子树发送一次取消。"""
        if self._task_cancellation_sent:
            return
        self._task_cancellation_sent = True
        if isinstance(self._current, _ManagedTask):
            self._current.request_cancel()
        for item in self._effects:
            if isinstance(item, _ManagedTask):
                item.request_cancel()
            elif isinstance(item, _ChildLink):
                item.child._cancel_tree()

    def _pump_sync(self) -> None:
        """同步清理在当前调用里做完，避免让出事件循环后租约还没释放。"""
        while self._current is not None or self._effects:
            if self._current is None:
                self._current = self._effects.popitem()[0]
            outcome = self._current.poll(self._cause)
            if isinstance(outcome, _Pending):
                if self._driver is None:
                    self._driver = asyncio.get_running_loop().create_task(
                        self._drain_async(outcome)
                    )
                return
            self._record(outcome)
            self._current = None
        self._complete()

    async def _drain_async(self, pending: _Pending) -> None:
        current: _Pending | None = pending
        try:
            while True:
                try:
                    while current is not None:
                        self._record(await current.wait())
                        self._current = None
                        current = self._next_pending()
                    return
                except asyncio.CancelledError as exc:
                    self._remember_cancel(exc)
                    self._current = None
                    current = self._next_pending()
                except Exception as exc:  # noqa: BLE001 - 驱动失败也要结束共享事务
                    self._record(exc)
                    self._current = None
                    current = self._next_pending()
        finally:
            if self._state is not EffectScopeState.CLOSED:
                self._current = None
                self._complete()

    def _next_pending(self) -> _Pending | None:
        while self._effects:
            self._current = self._effects.popitem()[0]
            outcome = self._current.poll(self._cause)
            if isinstance(outcome, _Pending):
                return outcome
            self._record(outcome)
            self._current = None
        self._complete()
        return None

    def _record(self, outcome: _Outcome) -> None:
        if isinstance(outcome, _Flatten):
            self._errors_list.extend(outcome.errors)
        elif isinstance(outcome, _PropagatedCancel):
            self._errors_list.extend(outcome.errors)
            self._remember_cancel(outcome.error)
        elif isinstance(outcome, asyncio.CancelledError):
            self._remember_cancel(outcome)
        elif outcome is not None:
            self._errors_list.append(outcome)

    def _remember_cancel(self, exc: asyncio.CancelledError) -> None:
        if self._cancel is None:
            self._cancel = exc

    def _complete(self) -> None:
        if self._state is EffectScopeState.CLOSED:
            return
        self._result = _CloseResult(tuple(self._errors_list), self._cancel)
        self._state = EffectScopeState.CLOSED
        future = self._future
        if future is not None and not future.done():
            future.set_result(self._result)
        self._future = None
        self._driver = None
        self._current = None
        self._effects.clear()

    def _exit_args(self) -> _ExitArgs:
        exc = self._cause
        if exc is None:
            return (None, None, None)
        return (type(exc), exc, exc.__traceback__)

    def _reject_if_closed(self) -> None:
        scope: ActivationScope | None = self
        while scope is not None:
            if scope._state is not EffectScopeState.OPEN:
                raise PluginActivationError(
                    "cannot register cleanup after the activation scope is closed",
                    plugin_id=self._plugin_id,
                    instance_id=self._instance_id,
                    phase="effects",
                    source="activation-scope",
                )
            scope = scope._parent


class EffectHandle:
    """一项已登记 cleanup 的句柄。重复关闭等待同一次结果。"""

    def __init__(self, effect: _Effect) -> None:
        self._effect = effect

    @property
    def label(self) -> str | None:
        return self._effect.label

    @property
    def active(self) -> bool:
        return not self._effect.finished

    async def aclose(self) -> None:
        """提前执行一次 cleanup；普通异常只由首次领取者抛出。"""
        if any(frame.effect() is self._effect for frame in _active_cleanup_frames()):
            raise PluginActivationError(
                "a cleanup cannot close its own handle",
                plugin_id=None,
                instance_id=None,
                phase="effects",
                source="activation-scope",
            )
        error = await self._effect.wait_for_handle()
        if error is not None:
            raise error


class TaskHandle(Generic[T]):
    """受管任务的句柄。所有权仍在 Scope 上。"""

    def __init__(self, item: _ManagedTask) -> None:
        self._item = item

    @property
    def label(self) -> str | None:
        return self._item.label

    @property
    def done(self) -> bool:
        return self._item.task.done()

    @property
    def task(self) -> asyncio.Task[T]:
        """返回可 await、查询结果或取消的任务；Scope 仍负责关闭它。"""
        return cast(asyncio.Task[T], self._item.task)

    async def aclose(self) -> None:
        """取消并等待任务；普通异常由首次领取者原样抛出。"""
        if any(
            frame.live() and frame.task is not None and frame.task() is self._item.task
            for frame in _task_frames.get()
        ):
            raise PluginActivationError(
                "a managed task cannot close its own handle",
                plugin_id=None,
                instance_id=None,
                phase="effects",
                source="activation-scope",
            )
        error = await self._item.wait_for_handle()
        if error is not None:
            raise error


EffectScope = ActivationScope


class EffectFacade:
    """当前插件实例的资源登记入口，资源随激活 Scope 一同关闭。"""

    def __init__(self, scope: ActivationScope) -> None:
        self._scope = scope

    def defer(self, cleanup: Cleanup, label: str | None = None) -> EffectHandle:
        """登记无参数的同步或异步清理函数，并返回可提前关闭的句柄。"""
        return self._scope.defer(cleanup, label=label)

    async def enter_async_context(
        self,
        manager: _AsyncResource[T],
        *,
        label: str | None = None,
    ) -> T:
        """进入异步上下文；进入成功后才登记它的退出方法。"""
        return await self._scope.enter_async_context(manager, label=label)

    def child_scope(self, *, label: str | None = None) -> ActivationScope:
        """创建随父 Scope 关闭、也可单独关闭的子 Scope。"""
        return self._scope.child_scope(label=label)

    def create_task(
        self,
        coroutine: Coroutine[object, object, T],
        *,
        label: str | None = None,
    ) -> TaskHandle[T]:
        """接管尚未调度的协程；Scope 关闭时取消并等待该任务。"""
        return self._scope.create_task(coroutine, label=label)


class PluginContext:
    """当前插件实例的身份、只读配置、服务、事件和资源登记入口。"""

    def __init__(
        self,
        plugin_id: str,
        instance_id: str,
        config: Mapping[str, object],
        scope: ActivationScope,
        services: ServiceFacade,
        capabilities: Iterable[str] = (),
        events: EventFacade | None = None,
        extensions: ExtensionFacade | None = None,
    ) -> None:
        self.plugin_id = plugin_id
        self.instance_id = instance_id
        self.config = config
        self.effects = EffectFacade(scope)
        self.services = services
        self._capabilities = frozenset(capabilities)
        self._events = events
        self._extensions = extensions

    @property
    def capabilities(self) -> frozenset[str]:
        """本次激活中已获授权、且由本插件声明的能力。"""
        return self._capabilities

    @property
    def events(self) -> EventFacade:
        """与本次 Activation 绑定的事件访问门面。"""
        if self._events is None:
            raise RuntimeError("event facade is not attached to a host")
        return self._events

    @property
    def extensions(self) -> ExtensionFacade:
        """与本次 Activation 绑定的观察型扩展访问门面。"""
        if self._extensions is None:
            raise RuntimeError("extension facade is not attached to a host")
        return self._extensions
