"""Fault-injection tests for M3b task ownership and shutdown."""

import asyncio
import gc
import inspect
import threading
import weakref

import pytest

from soul_plugin_core.context import ActivationScope
from soul_plugin_core.errors import PluginActivationError


def test_finished_tasks_release_results_without_closing_scope() -> None:
    class Result:
        pass

    async def scenario() -> None:
        scope = ActivationScope("worker", "worker-1")
        references: list[weakref.ReferenceType[Result]] = []

        async def work() -> Result:
            result = Result()
            references.append(weakref.ref(result))
            return result

        for _ in range(100):
            handle = scope.create_task(work())
            await handle.task
        # A retained handle still exposes its task and result.
        assert handle.task.result() is references[-1]()
        del handle
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        gc.collect()
        assert all(reference() is None for reference in references)
        assert await scope.close() == ()

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_completed_failures_keep_lifo_cleanup_order() -> None:
    async def scenario() -> None:
        scope = ActivationScope("worker", "worker-1")
        first = RuntimeError("first task")
        cleanup = RuntimeError("cleanup")
        last = RuntimeError("last task")

        async def fail(error: RuntimeError) -> None:
            raise error

        def fail_cleanup() -> None:
            raise cleanup

        one = scope.create_task(fail(first))
        scope.defer(fail_cleanup)
        two = scope.create_task(fail(last))
        await asyncio.gather(one.task, two.task, return_exceptions=True)
        await asyncio.sleep(0)
        assert await scope.close() == (last, cleanup, first)
        await one.aclose()
        await two.aclose()

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_retained_finished_task_handle_does_not_keep_scope_alive() -> None:
    async def scenario() -> None:
        scope = ActivationScope("worker", "worker-1")
        scope_reference = weakref.ref(scope)

        async def work() -> int:
            return 7

        handle = scope.create_task(work())
        assert await handle.task == 7
        await asyncio.sleep(0)
        del scope
        gc.collect()
        assert scope_reference() is None
        assert handle.task.result() == 7
        await handle.aclose()

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


@pytest.mark.parametrize("first_closer", ["handle", "scope"])
def test_task_failing_during_cancel_is_reported_once(first_closer: str) -> None:
    async def scenario() -> None:
        scope = ActivationScope("worker", "worker-1")
        started = asyncio.Event()
        failure = RuntimeError("task cancellation failed")

        async def worker() -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise failure

        handle = scope.create_task(worker())
        await started.wait()
        if first_closer == "handle":
            with pytest.raises(RuntimeError) as caught:
                await handle.aclose()
            assert caught.value is failure
            assert await scope.close() == ()
        else:
            assert await scope.close() == (failure,)
            await handle.aclose()

        await handle.aclose()

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_cancelled_handle_waiter_does_not_claim_task_failure() -> None:
    async def scenario() -> None:
        scope = ActivationScope("worker", "worker-1")
        started = asyncio.Event()
        cancelling = asyncio.Event()
        release = asyncio.Event()
        failure = RuntimeError("late task failure")

        async def worker() -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelling.set()
                await release.wait()
                raise failure

        handle = scope.create_task(worker())
        await started.wait()
        waiter = asyncio.create_task(handle.aclose())
        await cancelling.wait()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert not handle.done
        release.set()
        with pytest.raises(RuntimeError) as caught:
            await handle.aclose()
        assert caught.value is failure
        assert await scope.close() == ()

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_parent_close_does_not_cancel_child_task_twice() -> None:
    async def scenario() -> None:
        parent = ActivationScope("worker", "worker-1")
        child = parent.child_scope(label="child")
        started = asyncio.Event()
        cancelling = asyncio.Event()
        release = asyncio.Event()
        calls: list[str] = []

        async def worker() -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                calls.append("cancel")
                cancelling.set()
                await release.wait()
                raise

        handle = child.create_task(worker())
        await started.wait()
        child_close = asyncio.create_task(child.aclose())
        await cancelling.wait()
        parent_close = asyncio.create_task(parent.close())
        await asyncio.sleep(0)
        assert not handle.done
        release.set()
        await child_close
        assert await parent_close == ()
        assert calls == ["cancel"]

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_started_coroutine_is_rejected_without_taking_ownership() -> None:
    async def scenario() -> None:
        scope = ActivationScope("worker", "worker-1")
        started = asyncio.Event()

        async def worker() -> None:
            started.set()
            await asyncio.Event().wait()

        foreign = asyncio.create_task(worker())
        try:
            await started.wait()
            coroutine = foreign.get_coro()
            with pytest.raises(PluginActivationError) as caught:
                scope.create_task(coroutine)
            assert caught.value.phase == "effects"
            assert caught.value.plugin_id == "worker"
            assert caught.value.instance_id == "worker-1"
            assert inspect.getcoroutinestate(coroutine) == inspect.CORO_SUSPENDED
            assert not foreign.done()
            assert await scope.close() == ()
            assert not foreign.done()
        finally:
            foreign.cancel()
            with pytest.raises(asyncio.CancelledError):
                await foreign

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


@pytest.mark.parametrize("closed_scope", [False, True])
def test_already_scheduled_coroutine_is_not_adopted_or_closed(closed_scope: bool) -> None:
    async def scenario() -> None:
        scope = ActivationScope("worker", "worker-1")
        release = asyncio.Event()

        async def worker() -> int:
            await release.wait()
            return 7

        coroutine = worker()
        foreign = asyncio.create_task(coroutine)
        try:
            assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CREATED
            if closed_scope:
                assert await scope.close() == ()
            with pytest.raises(PluginActivationError):
                scope.create_task(coroutine)
            assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CREATED
            await asyncio.sleep(0)
            assert not foreign.done()
            release.set()
            assert await foreign == 7
            assert await scope.close() == ()
        finally:
            if not foreign.done():
                foreign.cancel()
                await asyncio.gather(foreign, return_exceptions=True)

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_replaced_all_tasks_uses_public_check(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        scope = ActivationScope("worker", "worker-1")
        release = asyncio.Event()
        original_all_tasks = asyncio.all_tasks
        calls: list[asyncio.AbstractEventLoop | None] = []

        def wrapped_all_tasks(
            loop: asyncio.AbstractEventLoop | None = None,
        ) -> set[asyncio.Task[object]]:
            calls.append(loop)
            return original_all_tasks(loop)

        async def worker() -> None:
            await release.wait()

        coroutine = worker()
        foreign = asyncio.create_task(coroutine)
        monkeypatch.setattr(asyncio, "all_tasks", wrapped_all_tasks)
        try:
            with pytest.raises(PluginActivationError):
                scope.create_task(coroutine)
            assert calls == [asyncio.get_running_loop()]
            assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CREATED
            release.set()
            await foreign
            assert await scope.close() == ()
        finally:
            if not foreign.done():
                foreign.cancel()
                await asyncio.gather(foreign, return_exceptions=True)

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_callable_all_tasks_replacement_uses_public_check(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        scope = ActivationScope("worker", "worker-1")
        original_all_tasks = asyncio.all_tasks
        calls: list[asyncio.AbstractEventLoop | None] = []

        class AllTasks:
            __module__ = "asyncio.tasks"
            __globals__ = None

            def __call__(
                self, loop: asyncio.AbstractEventLoop | None = None
            ) -> set[asyncio.Task[object]]:
                calls.append(loop)
                return original_all_tasks(loop)

        async def worker() -> int:
            return 7

        replacement = AllTasks()
        monkeypatch.setattr(asyncio, "all_tasks", replacement)
        monkeypatch.setattr(asyncio.tasks, "all_tasks", replacement)
        handle = scope.create_task(worker())
        assert calls == [asyncio.get_running_loop()]
        assert await handle.task == 7
        assert await scope.close() == ()

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_custom_task_factory_preserves_foreign_coroutine_ownership() -> None:
    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        scope = ActivationScope("worker", "worker-1")
        release = asyncio.Event()
        calls: list[str] = []

        class CustomTask(asyncio.Task[None]):
            def get_coro(self):
                calls.append("get_coro")
                return super().get_coro()

        async def worker() -> None:
            await release.wait()

        coroutine = worker()
        previous_factory = loop.get_task_factory()
        loop.set_task_factory(lambda active_loop, coro: CustomTask(coro, loop=active_loop))
        try:
            foreign = asyncio.create_task(coroutine)
        finally:
            loop.set_task_factory(previous_factory)
        try:
            with pytest.raises(PluginActivationError):
                scope.create_task(coroutine)
            assert calls
            assert not foreign.done()
            assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CREATED
            release.set()
            await foreign
            assert await scope.close() == ()
        finally:
            if not foreign.done():
                foreign.cancel()
                await asyncio.gather(foreign, return_exceptions=True)

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_other_loop_custom_task_is_not_inspected() -> None:
    started = threading.Event()
    other_loop = asyncio.new_event_loop()
    tasks: list[asyncio.Task[None]] = []
    errors: list[Exception] = []

    class OtherLoopTask(asyncio.Task[None]):
        def get_coro(self):
            raise AssertionError("a task from another loop was inspected")

    def run_other_loop() -> None:
        asyncio.set_event_loop(other_loop)

        async def pending() -> None:
            await asyncio.Event().wait()

        task = OtherLoopTask(pending(), loop=other_loop)
        tasks.append(task)
        started.set()
        try:
            other_loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # noqa: BLE001 - 将线程中的失败交给测试线程断言
            errors.append(exc)
        finally:
            other_loop.close()

    async def scenario() -> None:
        scope = ActivationScope("worker", "worker-1")

        async def worker() -> int:
            return 7

        handle = scope.create_task(worker())
        assert await handle.task == 7
        assert await scope.close() == ()

    thread = threading.Thread(target=run_other_loop)
    thread.start()
    try:
        assert started.wait(timeout=2)
        asyncio.run(asyncio.wait_for(scenario(), timeout=2))
    finally:
        if tasks:
            other_loop.call_soon_threadsafe(tasks[0].cancel)
        thread.join(timeout=2)
    assert not thread.is_alive()
    assert not errors


def test_unscheduled_coroutine_is_closed_when_no_event_loop_is_running() -> None:
    async def worker() -> None:
        return None

    scope = ActivationScope("worker", "worker-1")
    coroutine = worker()
    with pytest.raises(RuntimeError, match="no running event loop"):
        scope.create_task(coroutine)
    assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED


def test_background_failure_is_retrieved_without_waiting_for_scope_close() -> None:
    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        previous = loop.get_exception_handler()
        reports: list[dict[str, object]] = []
        loop.set_exception_handler(lambda _loop, context: reports.append(context))

        async def spawn_and_forget() -> weakref.ReferenceType[asyncio.Task[None]]:
            scope = ActivationScope("worker", "worker-1")

            async def worker() -> None:
                raise RuntimeError("background failure")

            handle = scope.create_task(worker())
            task_ref = weakref.ref(handle.task)
            await asyncio.sleep(0)
            assert handle.done
            await asyncio.sleep(0)
            return task_ref

        try:
            task_ref = await spawn_and_forget()
            gc.collect()
            await asyncio.sleep(0)
            assert task_ref() is None
            assert not any("never retrieved" in str(item.get("message", "")) for item in reports)
        finally:
            loop.set_exception_handler(previous)

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))
