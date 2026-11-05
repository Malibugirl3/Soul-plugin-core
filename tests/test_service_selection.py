"""Static strategy selection keeps service leases and never silently falls back."""

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
from soul_plugin_core.selection import ServiceSelectionError, ServiceSelector
from soul_plugin_core.services import ServiceHandle


class Policy(Protocol):
    async def choose(self, items: tuple[str, ...]) -> str: ...


def test_selects_declared_strategy_and_preserves_handle_lifetime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    selectors: list[ServiceSelector[Policy]] = []

    class First:
        async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
            context.services.provide("policy.first", self)

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def choose(self, items: tuple[str, ...]) -> str:
            return items[0]

    class Last:
        async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
            context.services.provide("policy.last", self)

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def choose(self, items: tuple[str, ...]) -> str:
            return items[-1]

    class Consumer:
        async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
            first = context.services.require("policy.first", Policy)
            last = context.services.require("policy.last", Policy)
            choices = {"first": first, "last": last}
            selector = ServiceSelector(choices, default="first")
            choices.clear()
            selectors.append(selector)

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

    module = ModuleType("service_selection_fixture")
    module.First, module.Last, module.Consumer = First, Last, Consumer  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)

    def definition(
        plugin_id: str,
        entry: str,
        *,
        provides: str | None = None,
        dependencies: tuple[str, ...] = (),
    ) -> PluginDefinition:
        return PluginDefinition(
            plugin_id=plugin_id,
            version="1.0",
            api_version="1",
            entry_point=f"{module.__name__}:{entry}",
            plugin_dependencies=(),
            service_dependencies=tuple(
                ServiceDependency(name, ">=1", False) for name in dependencies
            ),
            provided_services=(ProvidedService(provides, "1"),) if provides else (),
            capability_requests=(),
            config_schema=None,
            source=str(tmp_path / f"{plugin_id}.toml"),
        )

    definitions = (
        definition("first", "First", provides="policy.first"),
        definition("last", "Last", provides="policy.last"),
        definition("consumer", "Consumer", dependencies=("policy.first", "policy.last")),
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    host = PluginHost(definitions, build_activation_plan(definitions), config_path=config)

    async def scenario() -> None:
        await host.activate()
        selector = selectors[0]
        assert selector.available == ("first", "last")
        assert await selector.select().service.choose(("a", "b")) == "a"
        assert await selector.select("last").service.choose(("a", "b")) == "b"
        with pytest.raises(ServiceSelectionError) as caught:
            selector.select("missing")
        assert caught.value.name == "missing"
        assert caught.value.available == ("first", "last")
        old: ServiceHandle[Policy] = selector.select("first")
        await host.stop()
        with pytest.raises(ServiceUnavailableError):
            await old.service.choose(("a",))
        await host.dispose()

    asyncio.run(scenario())


def test_selector_requires_an_explicit_existing_default_and_snapshots_input() -> None:
    selector: ServiceSelector[Policy] = ServiceSelector({})
    with pytest.raises(ServiceSelectionError) as caught:
        selector.select()
    assert caught.value.name is None
    assert caught.value.available == ()
    with pytest.raises(ServiceSelectionError):
        ServiceSelector({}, default="missing")
    with pytest.raises(TypeError):
        ServiceSelector({"bad": object()})  # type: ignore[dict-item]
    with pytest.raises(ValueError):
        ServiceSelector({" ": object()})  # type: ignore[dict-item]
