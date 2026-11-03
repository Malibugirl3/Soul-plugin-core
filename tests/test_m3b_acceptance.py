"""M3b effect and task acceptance tests. Production behavior is not implemented yet."""

import asyncio
import gc
import sys
import warnings
import weakref
from pathlib import Path

import pytest

from soul_plugin_core.context import ActivationScope, EffectScopeState
from soul_plugin_core.contracts import PluginDefinition, PluginDependency
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import PluginActivationError, PluginCleanupError
from soul_plugin_core.host import PluginHost
from soul_plugin_core.lifecycle import FiberState


def run(awaitable):
    return asyncio.run(awaitable)


def definition(
    root: Path,
    plugin_id: str,
    module_name: str,
    *,
    plugins: tuple[PluginDependency, ...] = (),
) -> PluginDefinition:
    plugin_dir = root / plugin_id
    plugin_dir.mkdir(parents=True, exist_ok=True)
    manifest = plugin_dir / "plugin.toml"
    manifest.write_text("# source marker\n", encoding="utf-8")
    return PluginDefinition(
        plugin_id=plugin_id,
        version="1.0",
        api_version="1",
        entry_point=f"{module_name}:Plugin",
        plugin_dependencies=plugins,
        service_dependencies=(),
        provided_services=(),
        capability_requests=(),
        config_schema=None,
        source=str(manifest),
    )


def write_config(root: Path, instance_ids: tuple[str, ...]) -> Path:
    path = root / "config.toml"
    path.write_text(
        "".join(f"[plugins.{instance_id}]\n" for instance_id in instance_ids),
        encoding="utf-8",
    )
    return path


def write_module(root: Path, module_name: str, source: str) -> None:
    (root / f"{module_name}.py").write_text(source, encoding="utf-8")


def unload(*module_names: str) -> None:
    for module_name in module_names:
        sys.modules.pop(module_name, None)


def make_host(definitions, config_path: Path) -> PluginHost:
    stored = tuple(definitions)
    return PluginHost(
        stored,
        build_activation_plan(stored),
        config_path=config_path,
    )


def test_package_root_does_not_export_m3b_types() -> None:
    import soul_plugin_core

    assert soul_plugin_core.__all__ == ["__version__"]
    for name in ("EffectHandle", "EffectScope", "EffectScopeState", "TaskHandle"):
        assert not hasattr(soul_plugin_core, name)


def test_stop_can_defer_before_scope_closes(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_stop_defer",
        "ORDER = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        self.context = context\n"
        "        context.effects.defer(lambda: ORDER.append('setup'), label='setup')\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        ORDER.append('stop')\n"
        "        self.context.effects.defer(lambda: ORDER.append('from-stop'), label='stop')\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_stop_defer"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        run(host.activate())
        run(host.stop())
        assert sys.modules["m3b_stop_defer"].ORDER == ["stop", "from-stop", "setup"]
        assert host.get_state("owner") is FiberState.PENDING
    finally:
        unload("m3b_stop_defer")


def test_defer_handle_runs_once_and_scope_skips_it(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_handle_once",
        "CALLS = []\n"
        "ACTIVE = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        def work():\n"
        "            CALLS.append('work')\n"
        "        handle = context.effects.defer(work, label='work')\n"
        "        ACTIVE.append(handle.active)\n"
        "        ACTIVE.append(handle.label)\n"
        "        await handle.aclose()\n"
        "        await handle.aclose()\n"
        "        ACTIVE.append(handle.active)\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_handle_once"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        run(host.activate())
        run(host.stop())
        module = sys.modules["m3b_handle_once"]
        assert module.CALLS == ["work"]
        assert module.ACTIVE == [True, "work", False]
    finally:
        unload("m3b_handle_once")


def test_concurrent_handle_aclose_runs_cleanup_once(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_handle_concurrent",
        "import asyncio\n"
        "CALLS = []\n"
        "SAME = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        entered = asyncio.Event()\n"
        "        release = asyncio.Event()\n"
        "        async def work():\n"
        "            CALLS.append('enter')\n"
        "            entered.set()\n"
        "            await release.wait()\n"
        "            CALLS.append('exit')\n"
        "        handle = context.effects.defer(work, label='work')\n"
        "        try:\n"
        "            first = asyncio.create_task(handle.aclose())\n"
        "            await entered.wait()\n"
        "            second = asyncio.create_task(handle.aclose())\n"
        "            await asyncio.sleep(0)\n"
        "            assert not first.done()\n"
        "            assert not second.done()\n"
        "            release.set()\n"
        "            await first\n"
        "            await second\n"
        "            SAME.append(first.exception() is None and second.exception() is None)\n"
        "        finally:\n"
        "            release.set()\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_handle_concurrent"),),
        write_config(tmp_path, ("owner",)),
    )

    async def main():
        await asyncio.wait_for(host.activate(), timeout=2)
        await host.stop()

    try:
        run(main())
        module = sys.modules["m3b_handle_concurrent"]
        assert module.CALLS == ["enter", "exit"]
        assert module.SAME == [True]
    finally:
        unload("m3b_handle_concurrent")


def test_handle_aclose_raises_cleanup_exception(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_handle_error",
        "SEEN = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        def work():\n"
        "            raise RuntimeError('work-failed')\n"
        "        handle = context.effects.defer(work, label='work')\n"
        "        try:\n"
        "            await handle.aclose()\n"
        "        except RuntimeError as exc:\n"
        "            SEEN.append(str(exc))\n"
        "        SEEN.append(handle.active)\n"
        "        await handle.aclose()\n"
        "        SEEN.append('second')\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_handle_error"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        run(host.activate())
        run(host.stop())
        assert sys.modules["m3b_handle_error"].SEEN == ["work-failed", False, "second"]
    finally:
        unload("m3b_handle_error")


def test_interrupted_handle_aclose_resumes_without_repeating(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_handle_interrupt",
        "import asyncio\n"
        "RESULT = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        entered = asyncio.Event()\n"
        "        release = asyncio.Event()\n"
        "        calls = {'n': 0}\n"
        "        async def slow():\n"
        "            calls['n'] += 1\n"
        "            entered.set()\n"
        "            await release.wait()\n"
        "        handle = context.effects.defer(slow, label='slow')\n"
        "        try:\n"
        "            task = asyncio.create_task(handle.aclose())\n"
        "            await entered.wait()\n"
        "            task.cancel()\n"
        "            try:\n"
        "                await task\n"
        "                RESULT.append('missing-cancel')\n"
        "            except asyncio.CancelledError:\n"
        "                RESULT.append('cancelled')\n"
        "            RESULT.append(calls['n'])\n"
        "            release.set()\n"
        "            await handle.aclose()\n"
        "            RESULT.append(calls['n'])\n"
        "            RESULT.append(handle.active)\n"
        "        finally:\n"
        "            release.set()\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_handle_interrupt"),),
        write_config(tmp_path, ("owner",)),
    )

    async def main():
        await asyncio.wait_for(host.activate(), timeout=2)

    try:
        run(main())
        assert sys.modules["m3b_handle_interrupt"].RESULT == ["cancelled", 1, 1, False]
    finally:
        unload("m3b_handle_interrupt")


def test_cancelled_handle_aclose_reports_late_cleanup_error_once(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_handle_late_error",
        "import asyncio\n"
        "LATE = RuntimeError('late-failure')\n"
        "SEEN = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        entered = asyncio.Event()\n"
        "        release = asyncio.Event()\n"
        "        failed = asyncio.Event()\n"
        "        async def work():\n"
        "            entered.set()\n"
        "            await release.wait()\n"
        "            try:\n"
        "                raise LATE\n"
        "            finally:\n"
        "                failed.set()\n"
        "        handle = context.effects.defer(work, label='late')\n"
        "        try:\n"
        "            task = asyncio.create_task(handle.aclose())\n"
        "            await entered.wait()\n"
        "            task.cancel()\n"
        "            try:\n"
        "                await task\n"
        "                SEEN.append('missing-cancel')\n"
        "            except asyncio.CancelledError:\n"
        "                SEEN.append('cancelled')\n"
        "            release.set()\n"
        "            await failed.wait()\n"
        "            try:\n"
        "                await handle.aclose()\n"
        "                SEEN.append('missing-error')\n"
        "            except RuntimeError as exc:\n"
        "                SEEN.append(exc is LATE)\n"
        "            await handle.aclose()\n"
        "            SEEN.append('third')\n"
        "        finally:\n"
        "            release.set()\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_handle_late_error"),),
        write_config(tmp_path, ("owner",)),
    )

    async def main():
        await asyncio.wait_for(host.activate(), timeout=2)
        await host.stop()

    try:
        run(main())
        assert sys.modules["m3b_handle_late_error"].SEEN == ["cancelled", True, "third"]
    finally:
        unload("m3b_handle_late_error")


def test_scope_close_reports_late_cleanup_error_after_cancelled_handle(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_scope_late_error",
        "import asyncio\n"
        "LATE = RuntimeError('late-failure')\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        entered = asyncio.Event()\n"
        "        release = asyncio.Event()\n"
        "        failed = asyncio.Event()\n"
        "        async def work():\n"
        "            entered.set()\n"
        "            await release.wait()\n"
        "            try:\n"
        "                raise LATE\n"
        "            finally:\n"
        "                failed.set()\n"
        "        handle = context.effects.defer(work, label='late')\n"
        "        try:\n"
        "            task = asyncio.create_task(handle.aclose())\n"
        "            await entered.wait()\n"
        "            task.cancel()\n"
        "            try:\n"
        "                await task\n"
        "            except asyncio.CancelledError:\n"
        "                pass\n"
        "            release.set()\n"
        "            await failed.wait()\n"
        "        finally:\n"
        "            release.set()\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_scope_late_error"),),
        write_config(tmp_path, ("owner",)),
    )

    async def main():
        await asyncio.wait_for(host.activate(), timeout=2)
        await host.stop()

    try:
        with pytest.raises(PluginCleanupError) as caught:
            run(main())
        late = sys.modules["m3b_scope_late_error"].LATE
        assert caught.value.errors == (late,)
    finally:
        unload("m3b_scope_late_error")


def test_closing_scope_reports_caller_identity(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_closing_identity",
        "RAN = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        def during():\n"
        "            context.effects.defer(lambda: RAN.append('late'), label='late')\n"
        "        context.effects.defer(during, label='during')\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_closing_identity"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        run(host.activate())
        with pytest.raises(PluginCleanupError) as caught:
            run(host.stop())
        assert sys.modules["m3b_closing_identity"].RAN == []
        matches = [
            item
            for item in caught.value.errors
            if isinstance(item, PluginActivationError) and item.phase == "effects"
        ]
        assert len(matches) == 1
        assert matches[0].plugin_id == "owner"
        assert matches[0].instance_id == "owner"
    finally:
        unload("m3b_closing_identity")


def _resource_module(
    *, fail_enter: bool = False, truthy: bool = False, raise_after: bool = False
) -> str:
    enter = "raise RuntimeError('enter-failed')" if fail_enter else "return self"
    exit_value = "True" if truthy else "None"
    extra = ""
    if raise_after:
        extra = "        if exc_type is not None:\n            raise RuntimeError('exit-failed')\n"
    return (
        "EXITS = []\n"
        "class Box:\n"
        "    def __repr__(self):\n"
        "        return 'secret-token'\n"
        "    async def __aenter__(self):\n"
        "        " + enter + "\n"
        "    async def __aexit__(self, exc_type, exc, traceback):\n"
        "        name = None if exc_type is None else exc_type.__name__\n"
        "        text = None if exc is None else str(exc)\n"
        "        EXITS.append((name, text))\n" + extra + "        return " + exit_value + "\n"
    )


def test_async_enter_failure_does_not_exit(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_enter_fail",
        _resource_module(fail_enter=True)
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        await context.effects.enter_async_context(Box(), label='box')\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_enter_fail"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        with pytest.raises(PluginActivationError) as caught:
            run(host.activate())
        assert isinstance(caught.value.cause, RuntimeError)
        assert str(caught.value.cause) == "enter-failed"
        assert sys.modules["m3b_enter_fail"].EXITS == []
        assert host.get_state("owner") is FiberState.FAILED
    finally:
        unload("m3b_enter_fail")


def test_setup_failure_passes_original_cause_and_ignores_truthy_exit(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_setup_cause",
        _resource_module(truthy=True)
        + "STOPPED = []\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        await context.effects.enter_async_context(Box(), label='box')\n"
        + "        raise RuntimeError('root')\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        STOPPED.append('stop')\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_setup_cause"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        with pytest.raises(PluginActivationError) as caught:
            run(host.activate())
        assert isinstance(caught.value.cause, RuntimeError)
        assert str(caught.value.cause) == "root"
        assert caught.value.phase == "setup"
        assert sys.modules["m3b_setup_cause"].EXITS == [("RuntimeError", "root")]
        assert sys.modules["m3b_setup_cause"].STOPPED == []
        assert "secret-token" not in str(caught.value)
    finally:
        unload("m3b_setup_cause")


def test_start_failure_passes_original_cause_after_stop(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_start_cause",
        _resource_module()
        + "ORDER = []\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        await context.effects.enter_async_context(Box(), label='box')\n"
        + "    async def start(self):\n"
        + "        raise RuntimeError('start-root')\n"
        + "    async def stop(self):\n"
        + "        ORDER.append('stop')\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_start_cause"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        with pytest.raises(PluginActivationError) as caught:
            run(host.activate())
        module = sys.modules["m3b_start_cause"]
        assert isinstance(caught.value.cause, RuntimeError)
        assert str(caught.value.cause) == "start-root"
        assert module.ORDER == ["stop"]
        assert module.EXITS == [("RuntimeError", "start-root")]
    finally:
        unload("m3b_start_cause")


def test_normal_stop_passes_empty_cause(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_stop_cause",
        _resource_module()
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        await context.effects.enter_async_context(Box(), label='box')\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_stop_cause"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        run(host.activate())
        run(host.stop())
        assert sys.modules["m3b_stop_cause"].EXITS == [(None, None)]
        assert host.get_state("owner") is FiberState.PENDING
    finally:
        unload("m3b_stop_cause")


def test_early_child_close_passes_empty_cause(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_child_early",
        _resource_module()
        + "STATE = []\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        from soul_plugin_core.context import EffectScopeState\n"
        + "        child = context.effects.child_scope(label='child')\n"
        + "        STATE.append(child.state is EffectScopeState.OPEN)\n"
        + "        STATE.append(child.label)\n"
        + "        await child.enter_async_context(Box(), label='box')\n"
        + "        await child.aclose()\n"
        + "        STATE.append(child.state is EffectScopeState.CLOSED)\n"
        + "        try:\n"
        + "            child.defer(lambda: None, label='late')\n"
        + "        except PluginActivationError as exc:\n"
        + "            STATE.append((exc.phase, exc.plugin_id, exc.instance_id))\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        return None\n",
    )
    # PluginActivationError must be imported inside the generated module.
    source = (tmp_path / "m3b_child_early.py").read_text(encoding="utf-8")
    source = source.replace(
        "EXITS = []\n",
        "from soul_plugin_core.errors import PluginActivationError\nEXITS = []\n",
    )
    (tmp_path / "m3b_child_early.py").write_text(source, encoding="utf-8")
    host = make_host(
        (definition(tmp_path, "owner", "m3b_child_early"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        run(host.activate())
        run(host.stop())
        module = sys.modules["m3b_child_early"]
        assert module.EXITS == [(None, None)]
        assert module.STATE == [True, "child", True, ("effects", "owner", "owner")]
    finally:
        unload("m3b_child_early")


def test_healthy_rollback_passes_empty_cause(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_roll_provider",
        _resource_module()
        + "ORDER = []\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        await context.effects.enter_async_context(Box(), label='box')\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        ORDER.append('provider-stop')\n",
    )
    write_module(
        tmp_path,
        "m3b_roll_consumer",
        _resource_module()
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        await context.effects.enter_async_context(Box(), label='box')\n"
        + "        raise RuntimeError('consumer-broke')\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        raise AssertionError('consumer stop')\n",
    )
    provider = definition(tmp_path, "clock", "m3b_roll_provider")
    consumer = definition(
        tmp_path,
        "consumer",
        "m3b_roll_consumer",
        plugins=(PluginDependency("clock", ">=1", False),),
    )
    host = make_host((provider, consumer), write_config(tmp_path, ("clock", "consumer")))
    try:
        with pytest.raises(PluginActivationError) as caught:
            run(host.activate())
        assert str(caught.value.cause) == "consumer-broke"
        assert sys.modules["m3b_roll_consumer"].EXITS == [("RuntimeError", "consumer-broke")]
        assert sys.modules["m3b_roll_provider"].EXITS == [(None, None)]
        assert sys.modules["m3b_roll_provider"].ORDER == ["provider-stop"]
        assert host.get_state("clock") is FiberState.PENDING
        assert host.get_state("consumer") is FiberState.FAILED
    finally:
        unload("m3b_roll_provider", "m3b_roll_consumer")


def test_child_errors_flatten_in_lifo_order(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_child_flat",
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        def fail(name):\n"
        "            def cleanup():\n"
        "                raise RuntimeError(name)\n"
        "            return cleanup\n"
        "        context.effects.defer(fail('A'), label='A')\n"
        "        child = context.effects.child_scope(label='child')\n"
        "        child.defer(fail('B'), label='B')\n"
        "        child.defer(fail('C'), label='C')\n"
        "        context.effects.defer(fail('D'), label='D')\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_child_flat"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        run(host.activate())
        with pytest.raises(PluginCleanupError) as caught:
            run(host.stop())
        assert [str(item) for item in caught.value.errors] == ["D", "C", "B", "A"]
        assert not any(isinstance(item, PluginCleanupError) for item in caught.value.errors)
    finally:
        unload("m3b_child_flat")


def test_parent_close_rejects_child_registration(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_child_reject",
        "RAN = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        child = context.effects.child_scope(label='child')\n"
        "        def during():\n"
        "            child.defer(lambda: RAN.append('child'), label='late')\n"
        "        context.effects.defer(during, label='during')\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_child_reject"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        run(host.activate())
        with pytest.raises(PluginCleanupError) as caught:
            run(host.stop())
        assert sys.modules["m3b_child_reject"].RAN == []
        matches = [
            item
            for item in caught.value.errors
            if isinstance(item, PluginActivationError) and item.phase == "effects"
        ]
        assert matches
        assert matches[0].plugin_id == "owner"
        assert matches[0].instance_id == "owner"
    finally:
        unload("m3b_child_reject")


def test_concurrent_child_scope_aclose_runs_cleanup_once(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_scope_concurrent",
        "import asyncio\n"
        "CALLS = []\n"
        "SAME = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        entered = asyncio.Event()\n"
        "        release = asyncio.Event()\n"
        "        async def work():\n"
        "            CALLS.append('enter')\n"
        "            entered.set()\n"
        "            await release.wait()\n"
        "            CALLS.append('exit')\n"
        "        child = context.effects.child_scope(label='child')\n"
        "        child.defer(work, label='work')\n"
        "        try:\n"
        "            first = asyncio.create_task(child.aclose())\n"
        "            await entered.wait()\n"
        "            second = asyncio.create_task(child.aclose())\n"
        "            await asyncio.sleep(0)\n"
        "            assert not first.done()\n"
        "            assert not second.done()\n"
        "            release.set()\n"
        "            await first\n"
        "            await second\n"
        "            SAME.append(first.exception() is None and second.exception() is None)\n"
        "        finally:\n"
        "            release.set()\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_scope_concurrent"),),
        write_config(tmp_path, ("owner",)),
    )

    async def main():
        await asyncio.wait_for(host.activate(), timeout=2)
        await host.stop()

    try:
        run(main())
        module = sys.modules["m3b_scope_concurrent"]
        assert module.CALLS == ["enter", "exit"]
        assert module.SAME == [True]
    finally:
        unload("m3b_scope_concurrent")


def test_interrupted_child_scope_aclose_resumes_without_repeating(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_scope_interrupt",
        "import asyncio\n"
        "RESULT = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        from soul_plugin_core.context import EffectScopeState\n"
        "        entered = asyncio.Event()\n"
        "        release = asyncio.Event()\n"
        "        calls = {'marker': 0, 'slow': 0}\n"
        "        async def slow():\n"
        "            calls['slow'] += 1\n"
        "            entered.set()\n"
        "            await release.wait()\n"
        "        def marker():\n"
        "            calls['marker'] += 1\n"
        "        child = context.effects.child_scope(label='child')\n"
        "        child.defer(slow, label='slow')\n"
        "        child.defer(marker, label='marker')\n"
        "        try:\n"
        "            task = asyncio.create_task(child.aclose())\n"
        "            await entered.wait()\n"
        "            RESULT.append(calls['marker'])\n"
        "            task.cancel()\n"
        "            try:\n"
        "                await task\n"
        "                RESULT.append('missing-cancel')\n"
        "            except asyncio.CancelledError:\n"
        "                RESULT.append('cancelled')\n"
        "            RESULT.append(calls['marker'])\n"
        "            RESULT.append(calls['slow'])\n"
        "            release.set()\n"
        "            await child.aclose()\n"
        "            RESULT.append(calls['marker'])\n"
        "            RESULT.append(calls['slow'])\n"
        "            RESULT.append(child.state is EffectScopeState.CLOSED)\n"
        "        finally:\n"
        "            release.set()\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_scope_interrupt"),),
        write_config(tmp_path, ("owner",)),
    )

    async def main():
        await asyncio.wait_for(host.activate(), timeout=2)

    try:
        run(main())
        assert sys.modules["m3b_scope_interrupt"].RESULT == [
            1,
            "cancelled",
            1,
            1,
            1,
            1,
            True,
        ]
    finally:
        unload("m3b_scope_interrupt")


@pytest.mark.parametrize("delay", [False, True], ids=["immediate", "after-yield"])
def test_cleanup_cancellation_finishes_scope_and_propagates(delay: bool) -> None:
    async def scenario() -> None:
        scope = ActivationScope("owner", "owner")
        order: list[str] = []
        original = asyncio.CancelledError("cleanup-cancelled")

        async def boom() -> None:
            order.append("boom")
            if delay:
                await asyncio.sleep(0)
            raise original

        def tail() -> None:
            order.append("tail")

        scope.defer(tail, label="tail")
        scope.defer(boom, label="boom")

        async def close_and_check() -> None:
            # Python 3.10 may recreate CancelledError across Task boundaries.
            # Check Core's exception identity before wait_for's Task boundary.
            with pytest.raises(asyncio.CancelledError) as caught:
                await scope.close()
            assert caught.value is original

        await asyncio.wait_for(close_and_check(), timeout=2)
        assert order == ["boom", "tail"]
        assert scope.state is EffectScopeState.CLOSED
        assert await scope.close() == ()

    run(scenario())


def test_handle_aclose_claims_cleanup_cancel_once() -> None:
    async def scenario() -> None:
        scope = ActivationScope("owner", "owner")
        original = asyncio.CancelledError("cleanup-cancelled")

        async def boom() -> None:
            raise original

        handle = scope.defer(boom, label="boom")
        with pytest.raises(asyncio.CancelledError) as caught:
            await handle.aclose()
        assert caught.value is original
        assert await handle.aclose() is None
        assert await scope.close() == ()
        assert scope.state is EffectScopeState.CLOSED

    run(scenario())


def test_cancelled_close_waiter_does_not_leave_unretrieved_future() -> None:
    async def scenario() -> None:
        reports: list[dict[str, object]] = []
        loop = asyncio.get_running_loop()
        previous = loop.get_exception_handler()

        def handler(_loop: asyncio.AbstractEventLoop, context: dict[str, object]) -> None:
            reports.append(context)

        loop.set_exception_handler(handler)
        entered = asyncio.Event()
        release = asyncio.Event()
        original = asyncio.CancelledError("cleanup-cancelled")
        scope_ref = None

        async def boom() -> None:
            entered.set()
            await release.wait()
            raise original

        async def isolated() -> asyncio.Task[tuple[BaseException, ...]]:
            nonlocal scope_ref
            scope = ActivationScope("owner", "owner")
            scope_ref = weakref.ref(scope)
            scope.defer(boom, label="boom")
            waiter = asyncio.create_task(scope.close())
            await entered.wait()
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            return waiter

        try:
            waiter = await isolated()
            del waiter
            release.set()
            # 驱动任务还需要调度数轮才能完成。先等事务进入终态，
            # 不读取其 Future，再验证无人持有 Scope 时的事件循环诊断。
            for _ in range(20):
                assert scope_ref is not None
                scope = scope_ref()
                if scope is None:
                    break
                closed = scope.state is EffectScopeState.CLOSED
                del scope
                if closed:
                    break
                await asyncio.sleep(0)
            else:
                pytest.fail("scope close did not finish")
            gc.collect()
            assert scope_ref is not None
            assert scope_ref() is None
            rendered = " ".join(
                str(item.get("message", "")) + " " + str(item.get("exception", ""))
                for item in reports
            )
            assert "never retrieved" not in rendered
        finally:
            loop.set_exception_handler(previous)

    run(scenario())


def test_cleanup_cancel_does_not_replace_setup_error(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_setup_root_cancel",
        "import asyncio\n"
        "CANCEL = asyncio.CancelledError('cleanup')\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        async def boom():\n"
        "            raise CANCEL\n"
        "        context.effects.defer(boom, label='boom')\n"
        "        raise RuntimeError('setup root')\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        raise AssertionError('setup failed instance must not stop')\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_setup_root_cancel"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        with pytest.raises(PluginActivationError) as caught:
            run(host.activate())
        root = caught.value.cause
        assert isinstance(root, RuntimeError)
        assert str(root) == "setup root"
        assert caught.value is not sys.modules["m3b_setup_root_cancel"].CANCEL
        assert host.get_state("owner") is FiberState.FAILED
        assert host._fibers["owner"].instance is None
        assert host._fibers["owner"].scope is None
    finally:
        unload("m3b_setup_root_cancel")


def _two_instance_cancel_modules(tmp_path: Path) -> None:
    write_module(
        tmp_path,
        "m3b_pair_a",
        "ORDER = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        return None\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        ORDER.append('a:stop')\n",
    )
    write_module(
        tmp_path,
        "m3b_pair_b",
        "import asyncio\n"
        "ORDER = []\n"
        "CANCEL = asyncio.CancelledError('cleanup')\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        async def boom():\n"
        "            raise CANCEL\n"
        "        context.effects.defer(boom, label='boom')\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        ORDER.append('b:stop')\n",
    )


def test_cleanup_cancel_does_not_interrupt_other_instance_stop(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    _two_instance_cancel_modules(tmp_path)
    first = definition(tmp_path, "a", "m3b_pair_a")
    second = definition(
        tmp_path,
        "b",
        "m3b_pair_b",
        plugins=(PluginDependency("a", ">=1", False),),
    )
    host = make_host((first, second), write_config(tmp_path, ("a", "b")))

    async def stop_and_check() -> None:
        with pytest.raises(asyncio.CancelledError) as caught:
            await host.stop()
        assert caught.value is sys.modules["m3b_pair_b"].CANCEL

    try:
        run(host.activate())
        run(stop_and_check())
        assert sys.modules["m3b_pair_b"].ORDER == ["b:stop"]
        assert sys.modules["m3b_pair_a"].ORDER == ["a:stop"]
        assert host.get_state("a") is FiberState.PENDING
        assert host.get_state("b") is FiberState.PENDING
        assert host._fibers["a"].instance is None
        assert host._fibers["b"].instance is None
        assert host._fibers["a"].scope is None
        assert host._fibers["b"].scope is None
        assert "a" not in host._services._current
        assert "b" not in host._services._current
    finally:
        unload("m3b_pair_a", "m3b_pair_b")


def test_cleanup_cancel_does_not_interrupt_other_instance_dispose(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    _two_instance_cancel_modules(tmp_path)
    first = definition(tmp_path, "a", "m3b_pair_a")
    second = definition(
        tmp_path,
        "b",
        "m3b_pair_b",
        plugins=(PluginDependency("a", ">=1", False),),
    )
    host = make_host((first, second), write_config(tmp_path, ("a", "b")))

    async def dispose_and_check() -> None:
        with pytest.raises(asyncio.CancelledError) as caught:
            await host.dispose()
        assert caught.value is sys.modules["m3b_pair_b"].CANCEL

    try:
        run(host.activate())
        run(dispose_and_check())
        assert sys.modules["m3b_pair_b"].ORDER == ["b:stop"]
        assert sys.modules["m3b_pair_a"].ORDER == ["a:stop"]
        assert host.get_state("a") is FiberState.DISPOSED
        assert host.get_state("b") is FiberState.DISPOSED
        assert "a" not in host._services._current
        assert "b" not in host._services._current
    finally:
        unload("m3b_pair_a", "m3b_pair_b")


def test_cleanup_cancel_does_not_interrupt_rollback(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_roll_ok",
        "ORDER = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        return None\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        ORDER.append('provider-stop')\n",
    )
    write_module(
        tmp_path,
        "m3b_roll_cancel",
        "import asyncio\n"
        "CANCEL = asyncio.CancelledError('cleanup')\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        async def boom():\n"
        "            raise CANCEL\n"
        "        context.effects.defer(boom, label='boom')\n"
        "        raise RuntimeError('consumer-broke')\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        raise AssertionError('setup failed instance must not stop')\n",
    )
    provider = definition(tmp_path, "clock", "m3b_roll_ok")
    consumer = definition(
        tmp_path,
        "consumer",
        "m3b_roll_cancel",
        plugins=(PluginDependency("clock", ">=1", False),),
    )
    host = make_host((provider, consumer), write_config(tmp_path, ("clock", "consumer")))
    try:
        with pytest.raises(PluginActivationError) as caught:
            run(host.activate())
        assert str(caught.value.cause) == "consumer-broke"
        assert caught.value is not sys.modules["m3b_roll_cancel"].CANCEL
        assert sys.modules["m3b_roll_ok"].ORDER == ["provider-stop"]
        assert host.get_state("clock") is FiberState.PENDING
        assert host.get_state("consumer") is FiberState.FAILED
        assert host._fibers["clock"].instance is None
        assert host._fibers["consumer"].instance is None
        assert "clock" not in host._services._current
        assert "consumer" not in host._services._current
    finally:
        unload("m3b_roll_ok", "m3b_roll_cancel")


def test_cancelled_setup_passes_cause_and_keeps_original_error(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_cancel_setup",
        _resource_module(raise_after=True)
        + "import asyncio\n"
        + "import m3b_cancel_gate\n"
        + "STOPPED = []\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        await context.effects.enter_async_context(Box(), label='box')\n"
        + "        m3b_cancel_gate.ready.set()\n"
        + "        await asyncio.sleep(30)\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        STOPPED.append('stop')\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_cancel_setup"),),
        write_config(tmp_path, ("owner",)),
    )

    async def main():
        gate = type(sys)("m3b_cancel_gate")
        gate.ready = asyncio.Event()
        sys.modules["m3b_cancel_gate"] = gate
        task = asyncio.create_task(host.activate())
        ready_task = asyncio.create_task(gate.ready.wait())
        done, pending = await asyncio.wait(
            {task, ready_task},
            timeout=2,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for item in pending:
            item.cancel()
        if task in done:
            ready_task.cancel()
            await task
            raise AssertionError("activation finished before cancellation")
        if ready_task not in done:
            task.cancel()
            raise AssertionError("setup did not become ready to cancel")
        task.cancel()
        try:
            await task
        except asyncio.CancelledError as exc:
            assert exc.__cause__ is None
            return
        raise AssertionError("cancellation was replaced")

    try:
        run(main())
        module = sys.modules["m3b_cancel_setup"]
        assert [item[0] for item in module.EXITS] == ["CancelledError"]
        assert module.STOPPED == []
        assert host.get_state("owner") is FiberState.FAILED
    finally:
        unload("m3b_cancel_setup", "m3b_cancel_gate")


def test_cleanup_error_text_omits_resource_repr(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_repr",
        _resource_module()
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        await context.effects.enter_async_context(Box(), label='box')\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        return None\n",
    )
    source = (tmp_path / "m3b_repr.py").read_text(encoding="utf-8")
    source = source.replace(
        "return None\n",
        "raise RuntimeError('exit-failed')\n",
        1,
    )
    (tmp_path / "m3b_repr.py").write_text(source, encoding="utf-8")
    host = make_host(
        (definition(tmp_path, "owner", "m3b_repr"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        run(host.activate())
        with pytest.raises(PluginCleanupError) as caught:
            run(host.stop())
        rendered = str(caught.value) + "".join(str(item) for item in caught.value.errors)
        assert "secret-token" not in rendered
        assert any(
            isinstance(item, RuntimeError) and str(item) == "exit-failed"
            for item in caught.value.errors
        )
    finally:
        unload("m3b_repr")


def test_create_task_result_stays_readable(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_task_result",
        "VALUE = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        async def work():\n"
        "            return 7\n"
        "        handle = context.effects.create_task(work(), label='work')\n"
        "        VALUE.append(handle.label)\n"
        "        VALUE.append(await handle.task)\n"
        "        VALUE.append(handle.done)\n"
        "        VALUE.append(handle.task.result())\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_task_result"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        run(host.activate())
        run(host.stop())
        assert sys.modules["m3b_task_result"].VALUE == ["work", 7, True, 7]
        assert host.get_state("owner") is FiberState.PENDING
    finally:
        unload("m3b_task_result")


def test_task_failure_keeps_fiber_active_and_reports_once(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_task_fail",
        "CONSUMED = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        import asyncio\n"
        "        async def boom():\n"
        "            raise RuntimeError('boom')\n"
        "        handle = context.effects.create_task(boom(), label='boom')\n"
        "        await asyncio.sleep(0)\n"
        "        try:\n"
        "            await handle.task\n"
        "        except RuntimeError as exc:\n"
        "            CONSUMED.append(str(exc))\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_task_fail"),),
        write_config(tmp_path, ("owner",)),
    )

    async def main():
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await host.activate()
            assert host.get_state("owner") is FiberState.ACTIVE
            with pytest.raises(PluginCleanupError) as cleanup:
                await host.stop()
            gc.collect()
        return caught, cleanup.value

    try:
        caught, error = run(main())
        assert sys.modules["m3b_task_fail"].CONSUMED == ["boom"]
        booms = [
            item for item in error.errors if isinstance(item, RuntimeError) and str(item) == "boom"
        ]
        assert len(booms) == 1
        assert host.get_state("owner") is FiberState.FAILED
        assert not any("never retrieved" in str(item.message) for item in caught)
    finally:
        unload("m3b_task_fail")


def test_plugin_cancel_is_not_a_cleanup_error(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_task_cancel",
        "import asyncio\n"
        "DONE = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        async def nap():\n"
        "            await asyncio.sleep(30)\n"
        "        handle = context.effects.create_task(nap(), label='nap')\n"
        "        handle.task.cancel()\n"
        "        await asyncio.sleep(0)\n"
        "        DONE.append(handle.task.cancelled())\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_task_cancel"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        run(host.activate())
        assert sys.modules["m3b_task_cancel"].DONE == [True]
        assert host.get_state("owner") is FiberState.ACTIVE
        run(host.stop())
        assert host.get_state("owner") is FiberState.PENDING
    finally:
        unload("m3b_task_cancel")


def test_same_scope_tasks_are_cancelled_before_lifo_wait(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_task_batch",
        "import asyncio\n"
        "OLDER_STARTED = asyncio.Event()\n"
        "NEWER_STARTED = asyncio.Event()\n"
        "OLDER_CANCELLED = asyncio.Event()\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        async def older():\n"
        "            OLDER_STARTED.set()\n"
        "            try:\n"
        "                await asyncio.sleep(30)\n"
        "            except asyncio.CancelledError:\n"
        "                OLDER_CANCELLED.set()\n"
        "                raise\n"
        "        async def newer():\n"
        "            await OLDER_STARTED.wait()\n"
        "            NEWER_STARTED.set()\n"
        "            try:\n"
        "                await asyncio.sleep(30)\n"
        "            except asyncio.CancelledError:\n"
        "                await OLDER_CANCELLED.wait()\n"
        "                raise\n"
        "        context.effects.create_task(older(), label='older')\n"
        "        context.effects.create_task(newer(), label='newer')\n"
        "        await NEWER_STARTED.wait()\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_task_batch"),),
        write_config(tmp_path, ("owner",)),
    )

    async def main():
        await asyncio.wait_for(host.activate(), timeout=2)
        await asyncio.wait_for(host.stop(), timeout=2)

    try:
        run(main())
        assert host.get_state("owner") is FiberState.PENDING
    finally:
        unload("m3b_task_batch")


def test_nested_tasks_are_cancelled_before_parent_wait(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_task_nested",
        "import asyncio\n"
        "CHILD_STARTED = asyncio.Event()\n"
        "PARENT_STARTED = asyncio.Event()\n"
        "CHILD_CANCELLED = asyncio.Event()\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        child = context.effects.child_scope(label='child')\n"
        "        async def child_task():\n"
        "            CHILD_STARTED.set()\n"
        "            try:\n"
        "                await asyncio.sleep(30)\n"
        "            except asyncio.CancelledError:\n"
        "                CHILD_CANCELLED.set()\n"
        "                raise\n"
        "        async def parent_task():\n"
        "            await CHILD_STARTED.wait()\n"
        "            PARENT_STARTED.set()\n"
        "            try:\n"
        "                await asyncio.sleep(30)\n"
        "            except asyncio.CancelledError:\n"
        "                await CHILD_CANCELLED.wait()\n"
        "                raise\n"
        "        child.create_task(child_task(), label='child-task')\n"
        "        context.effects.create_task(parent_task(), label='parent-task')\n"
        "        await PARENT_STARTED.wait()\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_task_nested"),),
        write_config(tmp_path, ("owner",)),
    )

    async def main():
        await asyncio.wait_for(host.activate(), timeout=2)
        await asyncio.wait_for(host.stop(), timeout=2)

    try:
        run(main())
        assert host.get_state("owner") is FiberState.PENDING
    finally:
        unload("m3b_task_nested")


def test_task_handle_aclose_waits_for_running_task(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_task_aclose_wait",
        "import asyncio\n"
        "ORDER = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        async def nap():\n"
        "            ORDER.append('ran')\n"
        "            try:\n"
        "                await asyncio.sleep(30)\n"
        "            except asyncio.CancelledError:\n"
        "                ORDER.append('cancelled')\n"
        "                raise\n"
        "        handle = context.effects.create_task(nap(), label='nap')\n"
        "        await asyncio.sleep(0)\n"
        "        await handle.aclose()\n"
        "        ORDER.append(handle.task.cancelled())\n"
        "        await handle.aclose()\n"
        "        ORDER.append('second')\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_task_aclose_wait"),),
        write_config(tmp_path, ("owner",)),
    )

    async def main():
        await asyncio.wait_for(host.activate(), timeout=2)
        await host.stop()

    try:
        run(main())
        assert sys.modules["m3b_task_aclose_wait"].ORDER == ["ran", "cancelled", True, "second"]
        assert host.get_state("owner") is FiberState.PENDING
    finally:
        unload("m3b_task_aclose_wait")


def test_task_handle_aclose_is_idempotent_and_concurrent(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_task_aclose_once",
        "import asyncio\n"
        "CALLS = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        entered = asyncio.Event()\n"
        "        release = asyncio.Event()\n"
        "        async def work():\n"
        "            CALLS.append('run')\n"
        "            entered.set()\n"
        "            try:\n"
        "                await asyncio.sleep(30)\n"
        "            except asyncio.CancelledError:\n"
        "                CALLS.append('cancel')\n"
        "                current = asyncio.current_task()\n"
        "                uncancel = getattr(current, 'uncancel', None)\n"
        "                if uncancel is not None:\n"
        "                    while current.cancelling():\n"
        "                        uncancel()\n"
        "                await release.wait()\n"
        "                raise\n"
        "        handle = context.effects.create_task(work(), label='work')\n"
        "        try:\n"
        "            await entered.wait()\n"
        "            first = asyncio.create_task(handle.aclose())\n"
        "            await asyncio.sleep(0)\n"
        "            second = asyncio.create_task(handle.aclose())\n"
        "            await asyncio.sleep(0)\n"
        "            assert not first.done()\n"
        "            assert not second.done()\n"
        "            release.set()\n"
        "            await first\n"
        "            await second\n"
        "            await handle.aclose()\n"
        "        finally:\n"
        "            release.set()\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_task_aclose_once"),),
        write_config(tmp_path, ("owner",)),
    )

    async def main():
        await asyncio.wait_for(host.activate(), timeout=2)
        await host.stop()

    try:
        run(main())
        assert sys.modules["m3b_task_aclose_once"].CALLS == ["run", "cancel"]
        assert host.get_state("owner") is FiberState.PENDING
    finally:
        unload("m3b_task_aclose_once")


def test_task_handle_aclose_reports_task_exception(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_task_aclose_error",
        "import asyncio\n"
        "SEEN = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        async def boom():\n"
        "            raise RuntimeError('boom')\n"
        "        handle = context.effects.create_task(boom(), label='boom')\n"
        "        await asyncio.sleep(0)\n"
        "        try:\n"
        "            await handle.aclose()\n"
        "            SEEN.append('missing')\n"
        "        except RuntimeError as exc:\n"
        "            SEEN.append(str(exc))\n"
        "        await handle.aclose()\n"
        "        SEEN.append('second')\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_task_aclose_error"),),
        write_config(tmp_path, ("owner",)),
    )

    async def main():
        await asyncio.wait_for(host.activate(), timeout=2)
        await host.stop()

    try:
        run(main())
        assert sys.modules["m3b_task_aclose_error"].SEEN == ["boom", "second"]
        assert host.get_state("owner") is FiberState.PENDING
    finally:
        unload("m3b_task_aclose_error")


def test_task_handle_aclose_of_cancelled_task_returns(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_task_aclose_cancelled",
        "import asyncio\n"
        "SEEN = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        async def nap():\n"
        "            await asyncio.sleep(30)\n"
        "        handle = context.effects.create_task(nap(), label='nap')\n"
        "        await asyncio.sleep(0)\n"
        "        handle.task.cancel()\n"
        "        await asyncio.sleep(0)\n"
        "        SEEN.append(handle.task.cancelled())\n"
        "        await handle.aclose()\n"
        "        await handle.aclose()\n"
        "        SEEN.append('closed')\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_task_aclose_cancelled"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        run(host.activate())
        assert sys.modules["m3b_task_aclose_cancelled"].SEEN == [True, "closed"]
        run(host.stop())
        assert host.get_state("owner") is FiberState.PENDING
    finally:
        unload("m3b_task_aclose_cancelled")


def test_stop_may_create_task_and_scope_cancels_it(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_task_in_stop",
        "import asyncio\n"
        "ORDER = []\n"
        "HANDLE = None\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        self.context = context\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        async def background():\n"
        "            ORDER.append('ran')\n"
        "            try:\n"
        "                await asyncio.sleep(30)\n"
        "            except asyncio.CancelledError:\n"
        "                ORDER.append('cancelled')\n"
        "                raise\n"
        "        ORDER.append('stop')\n"
        "        global HANDLE\n"
        "        HANDLE = self.context.effects.create_task(background(), label='late')\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_task_in_stop"),),
        write_config(tmp_path, ("owner",)),
    )

    async def main():
        await host.activate()
        await asyncio.wait_for(host.stop(), timeout=2)

    try:
        run(main())
        module = sys.modules["m3b_task_in_stop"]
        assert module.ORDER[0] == "stop"
        assert module.HANDLE.task.cancelled()
        assert host.get_state("owner") is FiberState.PENDING
    finally:
        unload("m3b_task_in_stop")


def test_create_task_rejects_non_coroutine_and_closes_coroutine(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_task_reject",
        "CONTEXT = None\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        global CONTEXT\n"
        "        CONTEXT = context\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_task_reject"),),
        write_config(tmp_path, ("owner",)),
    )

    async def orphan():
        return 1

    try:
        run(host.activate())
        run(host.stop())
        context = sys.modules["m3b_task_reject"].CONTEXT
        with pytest.raises(PluginActivationError) as rejected:
            context.effects.create_task(123, label="nope")
        assert rejected.value.phase == "effects"
        assert rejected.value.plugin_id == "owner"
        assert rejected.value.instance_id == "owner"
        coro = orphan()
        with pytest.raises(PluginActivationError):
            context.effects.create_task(coro, label="closed")
        assert coro.cr_frame is None
    finally:
        unload("m3b_task_reject")


def test_create_task_rejects_existing_task_without_adopting_it(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3b_task_foreign",
        "import asyncio\n"
        "from soul_plugin_core.errors import PluginActivationError\n"
        "SEEN = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        async def work():\n"
        "            await asyncio.sleep(30)\n"
        "        foreign = asyncio.create_task(work())\n"
        "        try:\n"
        "            await asyncio.sleep(0)\n"
        "            try:\n"
        "                context.effects.create_task(foreign, label='foreign')\n"
        "                SEEN.append('accepted')\n"
        "            except PluginActivationError as exc:\n"
        "                SEEN.append((exc.phase, exc.plugin_id, exc.instance_id))\n"
        "            SEEN.append(foreign.cancelled())\n"
        "            SEEN.append(foreign.done())\n"
        "        finally:\n"
        "            foreign.cancel()\n"
        "            try:\n"
        "                await foreign\n"
        "            except asyncio.CancelledError:\n"
        "                pass\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    host = make_host(
        (definition(tmp_path, "owner", "m3b_task_foreign"),),
        write_config(tmp_path, ("owner",)),
    )
    try:
        run(host.activate())
        assert sys.modules["m3b_task_foreign"].SEEN == [
            ("effects", "owner", "owner"),
            False,
            False,
        ]
        run(host.stop())
        assert host.get_state("owner") is FiberState.PENDING
    finally:
        unload("m3b_task_foreign")
