"""A synchronous composition plan does not change async service ownership."""

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
from soul_plugin_core.errors import ServiceUnavailableError
from soul_plugin_core.host import PluginHost
from soul_plugin_core.pipeline import AsyncPipeline
from soul_plugin_core.services import ServiceHandle


class Transform(Protocol):
    async def apply(self, value: str) -> str: ...


class Workflow(Protocol):
    async def run(self, value: str) -> str: ...


def test_pipeline_runs_in_order_and_propagates_original_failure() -> None:
    events: list[str] = []
    failure = RuntimeError("bad transform")

    async def first(value: str) -> str:
        events.append("first")
        return value + "A"

    async def second(value: str) -> str:
        events.append("second")
        return value + "B"

    async def fail(value: str) -> str:
        events.append("fail")
        raise failure

    async def scenario() -> None:
        assert await AsyncPipeline[str](()).run("x") == "x"
        assert await AsyncPipeline((first, second)).run("x") == "xAB"
        events.clear()
        with pytest.raises(RuntimeError) as caught:
            await AsyncPipeline((first, fail, second)).run("x")
        assert caught.value is failure
        assert events == ["first", "fail"]

    asyncio.run(scenario())


def test_pipeline_cancellation_stops_following_steps() -> None:
    async def scenario() -> None:
        entered, release = asyncio.Event(), asyncio.Event()
        events: list[str] = []

        async def wait(value: str) -> str:
            entered.set()
            await release.wait()
            return value

        async def next_step(value: str) -> str:
            events.append("next")
            return value

        running = asyncio.create_task(AsyncPipeline((wait, next_step)).run("x"))
        await entered.wait()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        assert events == []

    asyncio.run(asyncio.wait_for(scenario(), timeout=3))


def test_facade_pipeline_uses_guarded_services_and_revokes_old_handle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handles: list[ServiceHandle[Workflow]] = []

    class Upper:
        async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
            context.services.provide("upper", self)

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def apply(self, value: str) -> str:
            return value.upper()

    class Suffix:
        async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
            context.services.provide("suffix", self)

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def apply(self, value: str) -> str:
            return value + "!"

    class Facade:
        def __init__(self) -> None:
            self.pipeline: AsyncPipeline[str] | None = None

        async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
            upper = context.services.require("upper", Transform)
            suffix = context.services.require("suffix", Transform)
            self.pipeline = AsyncPipeline((upper.service.apply, suffix.service.apply))
            context.services.provide("workflow", self)

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def run(self, value: str) -> str:
            assert self.pipeline is not None
            return await self.pipeline.run(value)

    class Consumer:
        async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
            handles.append(context.services.require("workflow", Workflow))

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

    module = ModuleType("pipeline_fixture")
    module.Upper, module.Suffix, module.Facade, module.Consumer = Upper, Suffix, Facade, Consumer  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)

    def definition(plugin_id, entry, provided=(), dependencies=()):
        return PluginDefinition(
            plugin_id=plugin_id,
            version="1.0",
            api_version="1",
            entry_point=f"{module.__name__}:{entry}",
            plugin_dependencies=(),
            service_dependencies=tuple(
                ServiceDependency(name, ">=1", False) for name in dependencies
            ),
            provided_services=tuple(ProvidedService(name, "1") for name in provided),
            capability_requests=(),
            config_schema=None,
            source=str(tmp_path / f"{plugin_id}.toml"),
        )

    definitions = (
        definition("upper", "Upper", ("upper",)),
        definition("suffix", "Suffix", ("suffix",)),
        definition("facade", "Facade", ("workflow",), ("upper", "suffix")),
        definition("consumer", "Consumer", dependencies=("workflow",)),
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    host = PluginHost(definitions, build_activation_plan(definitions), config_path=config)

    async def scenario() -> None:
        await host.activate()
        old = handles[0]
        assert await old.service.run("hello") == "HELLO!"
        await host.stop()
        with pytest.raises(ServiceUnavailableError):
            await old.service.run("hello")
        await host.dispose()

    asyncio.run(scenario())
