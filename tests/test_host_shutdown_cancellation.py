"""Cancellation of shutdown must not strand active instances or in-flight calls."""

import asyncio
import sys
from collections.abc import Mapping
from pathlib import Path
from types import ModuleType
from typing import Protocol

import pytest

from soul_plugin_core.context import PluginContext
from soul_plugin_core.contracts import PluginDefinition, ProvidedService, ServiceDependency
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import PluginCleanupError
from soul_plugin_core.host import PluginHost
from soul_plugin_core.lifecycle import FiberState
from soul_plugin_core.services import ServiceHandle


class SlowService(Protocol):
    async def work(self) -> int: ...


@pytest.mark.parametrize("operation", ["stop", "dispose"])
@pytest.mark.parametrize("phase", ["service_drain", "stop_hook", "scope_cleanup"])
def test_cancelled_shutdown_finishes_every_instance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str, phase: str
) -> None:
    async def scenario() -> None:
        entered, release = asyncio.Event(), asyncio.Event()
        events: list[str] = []
        observed_cancellations: list[asyncio.CancelledError] = []
        handles: list[ServiceHandle[SlowService]] = []

        class Base:
            async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
                pass

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                events.append("base:stop")

        class Provider:
            async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
                context.services.provide("slow", self)
                context.effects.defer(self.cleanup)

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                if phase == "stop_hook":
                    entered.set()
                    await release.wait()
                events.append("provider:stop")

            async def cleanup(self) -> None:
                if phase == "scope_cleanup":
                    entered.set()
                    await release.wait()
                events.append("provider:cleanup")

            async def work(self) -> int:
                entered.set()
                await release.wait()
                events.append("work:done")
                return 7

        class Consumer:
            async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
                handles.append(context.services.require("slow", SlowService))

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                events.append("consumer:stop")

        module = ModuleType("exchange_shutdown_fixture")
        module.Base = Base  # type: ignore[attr-defined]
        module.Provider = Provider  # type: ignore[attr-defined]
        module.Consumer = Consumer  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, module.__name__, module)

        def definition(
            plugin_id: str,
            entry: str,
            *,
            provided: tuple[ProvidedService, ...] = (),
            dependencies: tuple[ServiceDependency, ...] = (),
        ) -> PluginDefinition:
            return PluginDefinition(
                plugin_id=plugin_id,
                version="1.0",
                api_version="1",
                entry_point=f"{module.__name__}:{entry}",
                plugin_dependencies=(),
                service_dependencies=dependencies,
                provided_services=provided,
                capability_requests=(),
                config_schema=None,
                source=str(tmp_path / f"{plugin_id}.toml"),
            )

        definitions = (
            definition("base", "Base"),
            definition("provider", "Provider", provided=(ProvidedService("slow", "1"),)),
            definition(
                "consumer", "Consumer", dependencies=(ServiceDependency("slow", ">=1", False),)
            ),
        )
        config = tmp_path / "config.toml"
        config.write_text("", encoding="utf-8")
        host = PluginHost(definitions, build_activation_plan(definitions), config_path=config)
        await host.activate()

        call: asyncio.Task[int] | None = None
        if phase == "service_drain":
            call = asyncio.create_task(handles[0].service.work())
            await asyncio.wait_for(entered.wait(), timeout=3)

        async def shutdown() -> None:
            try:
                await getattr(host, operation)()
            except asyncio.CancelledError as exc:
                observed_cancellations.append(exc)
                raise

        stopping = asyncio.create_task(shutdown())
        try:
            if phase == "service_drain":
                while host.get_state("provider") is not FiberState.UNLOADING:
                    await asyncio.sleep(0)
                await asyncio.sleep(0)
            else:
                await asyncio.wait_for(entered.wait(), timeout=3)

            assert events[0] == "consumer:stop"
            assert "base:stop" not in events
            stopping.cancel("first")
            await asyncio.sleep(0)
            stopping.cancel("second")
            await asyncio.sleep(0)
            assert not stopping.done()
            assert host.get_state("provider") is FiberState.UNLOADING

            release.set()
            if call is not None:
                assert await asyncio.wait_for(call, timeout=3) == 7
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(stopping, timeout=3)
            assert len(observed_cancellations) == 1
            assert observed_cancellations[0].args == ("first",)
            assert events.index("provider:stop") < events.index("provider:cleanup")
            assert events.index("provider:cleanup") < events.index("base:stop")
            expected = FiberState.PENDING if operation == "stop" else FiberState.DISPOSED
            assert all(host.get_state(item.plugin_id) is expected for item in definitions)
        finally:
            release.set()
            if call is not None:
                await asyncio.gather(call, return_exceptions=True)
            await asyncio.gather(stopping, return_exceptions=True)
            await host.dispose()

    asyncio.run(asyncio.wait_for(scenario(), timeout=15))


@pytest.mark.parametrize("operation", ["stop", "dispose"])
def test_shutdown_error_after_cancel_is_reported_without_skipping_other_plugins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    async def scenario() -> None:
        entered, release = asyncio.Event(), asyncio.Event()
        events: list[str] = []
        failure = RuntimeError("stop failed")

        class Base:
            async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
                pass

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                events.append("base:stop")

        class Failing:
            async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
                context.effects.defer(lambda: events.append("failing:cleanup"))

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                entered.set()
                await release.wait()
                raise failure

        module = ModuleType("exchange_stop_error_fixture")
        module.Base = Base  # type: ignore[attr-defined]
        module.Failing = Failing  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, module.__name__, module)
        definitions = tuple(
            PluginDefinition(
                plugin_id=plugin_id,
                version="1.0",
                api_version="1",
                entry_point=f"{module.__name__}:{entry}",
                plugin_dependencies=(),
                service_dependencies=(),
                provided_services=(),
                capability_requests=(),
                config_schema=None,
                source=str(tmp_path / f"{plugin_id}.toml"),
            )
            for plugin_id, entry in (("base", "Base"), ("failing", "Failing"))
        )
        config = tmp_path / "config.toml"
        config.write_text("", encoding="utf-8")
        host = PluginHost(definitions, build_activation_plan(definitions), config_path=config)
        await host.activate()
        stopping = asyncio.create_task(getattr(host, operation)())
        try:
            await asyncio.wait_for(entered.wait(), timeout=3)
            stopping.cancel("cancel while stop is running")
            await asyncio.sleep(0)
            assert not stopping.done()
            release.set()
            with pytest.raises(PluginCleanupError) as caught:
                await asyncio.wait_for(stopping, timeout=3)
            assert caught.value.errors == (failure,)
            assert events == ["failing:cleanup", "base:stop"]
            expected_base = FiberState.PENDING if operation == "stop" else FiberState.DISPOSED
            expected_failing = FiberState.FAILED if operation == "stop" else FiberState.DISPOSED
            assert host.get_state("base") is expected_base
            assert host.get_state("failing") is expected_failing
        finally:
            release.set()
            await asyncio.gather(stopping, return_exceptions=True)
            await host.dispose()

    asyncio.run(asyncio.wait_for(scenario(), timeout=15))


@pytest.mark.parametrize("operation", ["stop", "dispose"])
@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_stop_hook_self_cancel_preserves_synchronous_cleanup_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str, cleanup_fails: bool
) -> None:
    async def scenario() -> None:
        events: list[str] = []
        failure = RuntimeError("cleanup failed")

        class Base:
            async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
                pass

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                events.append("base:stop")

        class Cancelling:
            async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
                def cleanup() -> None:
                    events.append("cleanup")
                    if cleanup_fails:
                        raise failure

                context.effects.defer(cleanup)

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                task = asyncio.current_task()
                assert task is not None
                task.cancel("plugin-cancel")
                events.append("stop-returned")

        module = ModuleType("exchange_self_cancel_fixture")
        module.Base = Base  # type: ignore[attr-defined]
        module.Cancelling = Cancelling  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, module.__name__, module)
        definitions = tuple(
            PluginDefinition(
                plugin_id=plugin_id,
                version="1.0",
                api_version="1",
                entry_point=f"{module.__name__}:{entry}",
                plugin_dependencies=(),
                service_dependencies=(),
                provided_services=(),
                capability_requests=(),
                config_schema=None,
                source=str(tmp_path / f"{plugin_id}.toml"),
            )
            for plugin_id, entry in (("base", "Base"), ("cancelling", "Cancelling"))
        )
        config = tmp_path / "config.toml"
        config.write_text("", encoding="utf-8")
        host = PluginHost(definitions, build_activation_plan(definitions), config_path=config)
        await host.activate()
        if cleanup_fails:
            with pytest.raises(PluginCleanupError) as caught:
                await getattr(host, operation)()
            assert caught.value.errors == (failure,)
        else:
            with pytest.raises(asyncio.CancelledError) as cancelled:
                await getattr(host, operation)()
            assert cancelled.value.args == ("plugin-cancel",)
        assert events == ["stop-returned", "cleanup", "base:stop"]
        expected = (
            FiberState.DISPOSED
            if operation == "dispose"
            else FiberState.FAILED
            if cleanup_fails
            else FiberState.PENDING
        )
        assert host.get_state("cancelling") is expected
        await host.dispose()

    asyncio.run(asyncio.wait_for(scenario(), timeout=15))
