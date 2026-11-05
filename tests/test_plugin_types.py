"""The optional plugin helpers retain the existing Host lifecycle contract."""

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
from soul_plugin_core.errors import PluginActivationError, ServiceUnavailableError
from soul_plugin_core.host import PluginHost
from soul_plugin_core.lifecycle import FiberState
from soul_plugin_core.plugins import BasePlugin, ServicePlugin
from soul_plugin_core.services import ServiceHandle


class Clock(Protocol):
    async def now(self) -> int: ...


def definition(
    root: Path,
    plugin_id: str,
    entry: str,
    *,
    provided: tuple[ProvidedService, ...] = (),
    services: tuple[ServiceDependency, ...] = (),
) -> PluginDefinition:
    return PluginDefinition(
        plugin_id=plugin_id,
        version="1.0",
        api_version="1",
        entry_point=f"plugin_types_fixture:{entry}",
        plugin_dependencies=(),
        service_dependencies=services,
        provided_services=provided,
        capability_requests=(),
        config_schema=None,
        source=str(root / f"{plugin_id}.toml"),
    )


def make_host(root: Path, definitions: tuple[PluginDefinition, ...]) -> PluginHost:
    config_path = root / "config.toml"
    config_path.write_text("", encoding="utf-8")
    return PluginHost(definitions, build_activation_plan(definitions), config_path=config_path)


def test_base_defaults_and_plain_plugin_work_together(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []

    class Minimal(BasePlugin):
        async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
            events.append("minimal:setup")

    class Plain:
        async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
            events.append("plain:setup")

        async def start(self) -> None:
            events.append("plain:start")

        async def stop(self) -> None:
            events.append("plain:stop")

    module = ModuleType("plugin_types_fixture")
    module.Minimal = Minimal  # type: ignore[attr-defined]
    module.Plain = Plain  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    definitions = (
        definition(tmp_path, "minimal", "Minimal"),
        definition(tmp_path, "plain", "Plain"),
    )
    host = make_host(tmp_path, definitions)

    async def scenario() -> None:
        await host.activate()
        assert host.get_state("minimal") is FiberState.ACTIVE
        assert host.get_state("plain") is FiberState.ACTIVE
        await host.stop()
        assert events == ["minimal:setup", "plain:setup", "plain:start", "plain:stop"]
        await host.dispose()

    asyncio.run(scenario())


def test_service_plugin_publishes_after_start_and_releases_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    handles: list[ServiceHandle[Clock]] = []

    class ClockImpl:
        async def now(self) -> int:
            return 42

    class Provider(ServicePlugin[Clock]):
        service_name = "clock"

        async def create_service(
            self, context: PluginContext, config: Mapping[str, object]
        ) -> Clock:
            context.effects.defer(lambda: events.append("provider:cleanup"))
            events.append("provider:create")
            return ClockImpl()

        async def start(self) -> None:
            events.append("provider:start")

        async def stop(self) -> None:
            events.append("provider:stop")

    class Consumer(BasePlugin):
        async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
            events.append("consumer:setup")
            handles.append(context.services.require("clock", Clock))

        async def stop(self) -> None:
            events.append("consumer:stop")

    module = ModuleType("plugin_types_fixture")
    module.Provider = Provider  # type: ignore[attr-defined]
    module.Consumer = Consumer  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    definitions = (
        definition(tmp_path, "provider", "Provider", provided=(ProvidedService("clock", "2.0"),)),
        definition(
            tmp_path, "consumer", "Consumer", services=(ServiceDependency("clock", ">=1", False),)
        ),
    )
    host = make_host(tmp_path, definitions)

    async def scenario() -> None:
        await host.activate()
        assert events == ["provider:create", "provider:start", "consumer:setup"]
        assert handles[0].version == "2.0"
        assert await handles[0].service.now() == 42
        await host.stop()
        assert events[-3:] == ["consumer:stop", "provider:stop", "provider:cleanup"]
        with pytest.raises(ServiceUnavailableError):
            await handles[0].service.now()
        await host.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["undeclared", "start"])
def test_service_plugin_failure_cleans_scope_and_follows_lifecycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    events: list[str] = []

    class Provider(ServicePlugin[Clock]):
        service_name = "clock"

        async def create_service(
            self, context: PluginContext, config: Mapping[str, object]
        ) -> Clock:
            context.effects.defer(lambda: events.append("cleanup"))
            return ClockImpl()

        async def start(self) -> None:
            events.append("start")
            raise RuntimeError("start failure")

        async def stop(self) -> None:
            events.append("stop")

    class ClockImpl:
        async def now(self) -> int:
            return 1

    module = ModuleType("plugin_types_fixture")
    module.Provider = Provider  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    service_name = "other" if failure == "undeclared" else "clock"
    definitions = (
        definition(
            tmp_path, "provider", "Provider", provided=(ProvidedService(service_name, "1"),)
        ),
    )
    host = make_host(tmp_path, definitions)

    async def scenario() -> None:
        with pytest.raises(PluginActivationError):
            await host.activate()
        assert host.get_state("provider") is FiberState.FAILED
        assert events == (["cleanup"] if failure == "undeclared" else ["start", "stop", "cleanup"])
        await host.dispose()

    asyncio.run(scenario())
