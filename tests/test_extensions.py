"""Lifecycle-managed observation extensions are isolated from ordinary events."""

import asyncio
import sys
from pathlib import Path
from types import ModuleType

import pytest

from soul_plugin_core.capabilities import CapabilityPolicy
from soul_plugin_core.contracts import PluginDefinition
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import CapabilityDeniedError, EventDispatchError, PluginActivationError
from soul_plugin_core.host import PluginHost
from soul_plugin_core.lifecycle import FiberState


def make_host(tmp_path: Path, entries: tuple[tuple[str, str, tuple[str, ...]], ...]) -> PluginHost:
    definitions = tuple(
        PluginDefinition(
            plugin_id=plugin_id,
            version="1.0",
            api_version="1",
            entry_point=f"extension_fixture:{entry}",
            plugin_dependencies=(),
            service_dependencies=(),
            provided_services=(),
            capability_requests=capabilities,
            config_schema=None,
            source=str(tmp_path / f"{plugin_id}.toml"),
        )
        for plugin_id, entry, capabilities in entries
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    return PluginHost(
        definitions,
        build_activation_plan(definitions),
        config_path=config,
        capability_policy=CapabilityPolicy({plugin_id: caps for plugin_id, _, caps in entries}),
    )


def fixture_module(monkeypatch: pytest.MonkeyPatch, **entries: type) -> None:
    module = ModuleType("extension_fixture")
    for name, entry in entries.items():
        setattr(module, name, entry)
    monkeypatch.setitem(sys.modules, module.__name__, module)


def test_observer_is_staged_until_start_and_revoked_after_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        entered, release = asyncio.Event(), asyncio.Event()
        contexts = {}
        calls = []

        class Invoker:
            async def setup(self, context, config):
                contexts["invoker"] = context

            async def start(self):
                pass

            async def stop(self):
                pass

        class Observer:
            async def setup(self, context, config):
                async def observe(value):
                    calls.append(value)

                contexts["registration"] = context.extensions.register("point", observe)

            async def start(self):
                entered.set()
                await release.wait()

            async def stop(self):
                pass

        fixture_module(monkeypatch, Invoker=Invoker, Observer=Observer)
        host = make_host(
            tmp_path,
            (("a", "Invoker", ("extensions.invoke",)), ("b", "Observer", ("extensions.register",))),
        )
        activating = asyncio.create_task(host.activate())
        try:
            await asyncio.wait_for(entered.wait(), timeout=3)
            await contexts["invoker"].extensions.invoke("point", "before")
            assert calls == []
            release.set()
            await asyncio.wait_for(activating, timeout=3)
            await contexts["invoker"].extensions.invoke("point", "after")
            assert calls == ["after"]
            assert contexts["registration"].active
            await host.stop()
            assert not contexts["registration"].active
            with pytest.raises(PluginActivationError):
                await contexts["invoker"].extensions.invoke("point", "stale")
        finally:
            release.set()
            await asyncio.gather(activating, return_exceptions=True)
            await host.dispose()

    asyncio.run(asyncio.wait_for(scenario(), timeout=10))


@pytest.mark.parametrize("failure", ["start", "cancel"])
def test_failed_activation_never_exposes_staged_observer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    async def scenario() -> None:
        entered, release = asyncio.Event(), asyncio.Event()
        contexts = {}
        calls = []

        class Invoker:
            async def setup(self, context, config):
                contexts["invoker"] = context

            async def start(self):
                pass

            async def stop(self):
                pass

        class Observer:
            async def setup(self, context, config):
                async def observe(value):
                    calls.append(value)

                context.extensions.register("point", observe)

            async def start(self):
                entered.set()
                await release.wait()
                raise RuntimeError("start failure")

            async def stop(self):
                pass

        fixture_module(monkeypatch, Invoker=Invoker, Observer=Observer)
        host = make_host(
            tmp_path,
            (("a", "Invoker", ("extensions.invoke",)), ("b", "Observer", ("extensions.register",))),
        )
        activating = asyncio.create_task(host.activate())
        try:
            await asyncio.wait_for(entered.wait(), timeout=3)
            await contexts["invoker"].extensions.invoke("point", "during")
            assert calls == []
            if failure == "cancel":
                activating.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await activating
            else:
                release.set()
                with pytest.raises(PluginActivationError):
                    await activating
            assert host.get_state("a") is FiberState.PENDING
            assert host.get_state("b") is FiberState.FAILED
            assert calls == []
        finally:
            release.set()
            await asyncio.gather(activating, return_exceptions=True)
            await host.dispose()

    asyncio.run(asyncio.wait_for(scenario(), timeout=10))


def test_observers_run_in_order_continue_on_error_and_can_unregister(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        calls = []
        contexts = {}
        failure = RuntimeError("observer failure")

        class Invoker:
            async def setup(self, context, config):
                contexts["invoker"] = context

            async def start(self):
                pass

            async def stop(self):
                pass

        class First:
            async def setup(self, context, config):
                async def observe(value):
                    calls.append("first")
                    raise failure

                contexts["first"] = context.extensions.register("point", observe)

            async def start(self):
                pass

            async def stop(self):
                pass

        class Second:
            async def setup(self, context, config):
                async def observe(value):
                    calls.append("second")

                context.extensions.register("point", observe)

            async def start(self):
                pass

            async def stop(self):
                pass

        fixture_module(monkeypatch, Invoker=Invoker, First=First, Second=Second)
        host = make_host(
            tmp_path,
            (
                ("a", "Invoker", ("extensions.invoke",)),
                ("b", "First", ("extensions.register",)),
                ("c", "Second", ("extensions.register",)),
            ),
        )
        await host.activate()
        with pytest.raises(EventDispatchError) as caught:
            await contexts["invoker"].extensions.invoke("point", None)
        assert caught.value.errors == (failure,)
        assert calls == ["first", "second"]
        await contexts["first"].aclose()
        calls.clear()
        await contexts["invoker"].extensions.invoke("point", None)
        assert calls == ["second"]
        await host.dispose()

    asyncio.run(asyncio.wait_for(scenario(), timeout=10))


def test_extension_and_event_permissions_and_channels_are_independent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        contexts = {}
        calls = []

        class Events:
            async def setup(self, context, config):
                contexts["events"] = context

                async def observe(value):
                    calls.append("event")

                context.events.subscribe("same", observe)

            async def start(self):
                pass

            async def stop(self):
                pass

        class Extensions:
            async def setup(self, context, config):
                contexts["extensions"] = context

                async def observe(value):
                    calls.append("extension")

                context.extensions.register("same", observe)

            async def start(self):
                pass

            async def stop(self):
                pass

        fixture_module(monkeypatch, Events=Events, Extensions=Extensions)
        host = make_host(
            tmp_path,
            (
                ("a", "Events", ("events.subscribe", "events.publish")),
                ("b", "Extensions", ("extensions.register", "extensions.invoke")),
            ),
        )
        await host.activate()
        await contexts["events"].events.publish("same", None)
        assert calls == ["event"]
        await contexts["extensions"].extensions.invoke("same", None)
        assert calls == ["event", "extension"]
        with pytest.raises(CapabilityDeniedError) as registration:
            contexts["events"].extensions.register("same", lambda value: None)
        assert registration.value.missing == ("extensions.register",)
        with pytest.raises(CapabilityDeniedError) as invocation:
            await contexts["events"].extensions.invoke("same", None)
        assert invocation.value.missing == ("extensions.invoke",)
        with pytest.raises(CapabilityDeniedError):
            contexts["extensions"].events.subscribe("same", lambda value: None)
        await host.dispose()

    asyncio.run(asyncio.wait_for(scenario(), timeout=10))


def test_stop_drains_observer_despite_repeated_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        contexts = {}
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []

        class Invoker:
            async def setup(self, context, config):
                contexts["invoker"] = context

            async def start(self):
                pass

            async def stop(self):
                calls.append("invoker:stop")

        class Observer:
            async def setup(self, context, config):
                async def observe(value):
                    calls.append("observer:enter")
                    entered.set()
                    await release.wait()
                    calls.append("observer:exit")

                context.extensions.register("point", observe)

            async def start(self):
                pass

            async def stop(self):
                calls.append("observer:stop")

        fixture_module(monkeypatch, Invoker=Invoker, Observer=Observer)
        host = make_host(
            tmp_path,
            (("a", "Invoker", ("extensions.invoke",)), ("b", "Observer", ("extensions.register",))),
        )
        await host.activate()
        invocation = asyncio.create_task(contexts["invoker"].extensions.invoke("point", None))
        stopping = None
        try:
            await asyncio.wait_for(entered.wait(), timeout=3)
            stopping = asyncio.create_task(host.stop())
            while host.get_state("b") is not FiberState.UNLOADING:
                await asyncio.sleep(0)
            await contexts["invoker"].extensions.invoke("point", None)
            assert calls == ["observer:enter"]
            stopping.cancel("first")
            await asyncio.sleep(0)
            stopping.cancel("second")
            await asyncio.sleep(0)
            assert not stopping.done()
            release.set()
            await asyncio.wait_for(invocation, timeout=3)
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(stopping, timeout=3)
            assert calls == ["observer:enter", "observer:exit", "observer:stop", "invoker:stop"]
            assert host.get_state("a") is FiberState.PENDING
            assert host.get_state("b") is FiberState.PENDING
        finally:
            release.set()
            await asyncio.gather(invocation, return_exceptions=True)
            if stopping is not None:
                await asyncio.gather(stopping, return_exceptions=True)
            await host.dispose()

    asyncio.run(asyncio.wait_for(scenario(), timeout=10))
