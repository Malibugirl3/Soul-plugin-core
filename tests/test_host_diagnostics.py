"""Public snapshots expose immutable deployment diagnostics without plugin objects."""

import asyncio
import gc
import sys
import weakref
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import ModuleType

import pytest

from soul_plugin_core.contracts import PluginDefinition, ProvidedService, ServiceDependency
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.host import InstanceSnapshot, PluginHost
from soul_plugin_core.instances import InstanceBindingSelection, InstanceDefinition
from soul_plugin_core.lifecycle import FiberState


def definition(name: str, *, provides=(), depends=()) -> PluginDefinition:
    return PluginDefinition(
        plugin_id=name,
        version="1",
        api_version="1",
        entry_point=f"diagnostics_fixture:{name.title()}",
        plugin_dependencies=(),
        service_dependencies=tuple(ServiceDependency("clock", ">=1", value) for value in depends),
        provided_services=tuple(ProvidedService(name, "1") for name in provides),
        capability_requests=(),
        config_schema=None,
        source=f"{name}/plugin.toml",
    )


def test_snapshot_without_activation_never_loads_config_or_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = definition("provider", provides=("clock",))
    consumer = definition("consumer", depends=(False,))
    host = PluginHost(
        (consumer, provider),
        build_activation_plan((consumer, provider)),
        config_path=tmp_path / "missing.toml",
    )
    monkeypatch.delitem(sys.modules, "diagnostics_fixture", raising=False)
    before = host.snapshot()
    assert host.instance_plan.order == ("provider", "consumer")
    assert before == (
        InstanceSnapshot("provider", "provider", FiberState.PENDING, False, (), ()),
        InstanceSnapshot(
            "consumer", "consumer", FiberState.PENDING, False, ("provider",), ("provider",)
        ),
    )
    assert "diagnostics_fixture" not in sys.modules
    with pytest.raises(FrozenInstanceError):
        before[0].disabled = True
    with pytest.raises(FrozenInstanceError):
        host.instance_plan.order = ()

    empty = PluginHost((), build_activation_plan(()), config_path=tmp_path / "missing.toml")
    assert empty.instance_plan.order == ()
    assert empty.snapshot() == ()


def test_snapshot_observes_multiple_instances_and_optional_consumers_without_retaining_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = definition("provider", provides=("clock",))
    strong = definition("strong", depends=(False,))
    optional = definition("optional", depends=(True,))
    definitions = (provider, strong, optional)
    refs = []

    class Provider:
        async def setup(self, context, config):
            self.secret = "NEVER_INCLUDE_PLUGIN_STATE"
            refs.append(weakref.ref(self))
            context.services.provide("clock", self)

        async def start(self):
            pass

        async def stop(self):
            pass

    class Strong:
        async def setup(self, context, config):
            pass

        async def start(self):
            pass

        async def stop(self):
            pass

    class Optional(Strong):
        pass

    module = ModuleType("diagnostics_fixture")
    module.Provider, module.Strong, module.Optional = Provider, Strong, Optional
    monkeypatch.setitem(sys.modules, module.__name__, module)
    instances = (
        InstanceDefinition("p-a", "provider"),
        InstanceDefinition("p-b", "provider"),
        InstanceDefinition(
            "c-strong", "strong", (InstanceBindingSelection("service", "clock", "p-a"),)
        ),
        InstanceDefinition(
            "c-opt", "optional", (InstanceBindingSelection("service", "clock", "p-a"),)
        ),
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    host = PluginHost(
        definitions, build_activation_plan(definitions), config_path=config, instances=instances
    )

    async def scenario() -> None:
        await host.activate()
        active = host.snapshot()
        assert tuple(item.instance_id for item in active) == host.instance_plan.order
        assert all(item.state is FiberState.ACTIVE for item in active)
        await host.deactivate("p-a")
        stopped = {item.instance_id: item for item in host.snapshot()}
        assert stopped["p-a"].disabled and stopped["p-a"].state is FiberState.PENDING
        assert stopped["p-b"].state is FiberState.ACTIVE
        assert stopped["c-strong"].state is FiberState.PENDING
        assert stopped["c-strong"].blockers == ("p-a",)
        assert stopped["c-strong"].binding_blockers == ("p-a",)
        assert stopped["c-opt"].state is FiberState.ACTIVE
        assert stopped["c-opt"].blockers == ()
        assert stopped["c-opt"].binding_blockers == ("p-a",)
        assert "NEVER_INCLUDE_PLUGIN_STATE" not in repr(stopped)
        assert all(item.state is FiberState.ACTIVE for item in active)
        await host.activate_instance("p-a")
        await host.activate()
        assert all(item.state is FiberState.ACTIVE for item in host.snapshot())
        await host.dispose()
        gc.collect()
        assert all(ref() is None for ref in refs)
        assert all(item.state is FiberState.DISPOSED for item in host.snapshot())
        assert stopped["c-strong"].state is FiberState.PENDING

    asyncio.run(scenario())
