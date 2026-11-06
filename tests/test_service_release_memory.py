"""Revoked service handles keep metadata, but not stopped implementations."""

import asyncio
import gc
import sys
import weakref
from pathlib import Path
from types import ModuleType
from typing import Protocol

import pytest

from soul_plugin_core.contracts import PluginDefinition, ProvidedService, ServiceDependency
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import ServiceUnavailableError
from soul_plugin_core.host import PluginHost
from soul_plugin_core.services import ServiceHandle


class Clock(Protocol):
    async def now(self) -> int: ...


def test_revoked_handle_and_saved_method_do_not_retain_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handles: list[ServiceHandle[Clock]] = []
    refs: list[weakref.ReferenceType[object]] = []
    entered = asyncio.Event()
    release = asyncio.Event()

    class Implementation:
        accesses = 0

        def __getattribute__(self, name):
            if name == "now":
                type(self).accesses += 1
            return object.__getattribute__(self, name)

        async def now(self) -> int:
            entered.set()
            await release.wait()
            return 7

    class Provider:
        async def setup(self, context, config):
            implementation = Implementation()
            refs.append(weakref.ref(implementation))
            context.services.provide("clock", implementation)

        async def start(self):
            pass

        async def stop(self):
            pass

    class Consumer:
        async def setup(self, context, config):
            handles.append(context.services.optional("clock", Clock))

        async def start(self):
            pass

        async def stop(self):
            pass

    module = ModuleType("service_release_memory_plugins")
    module.Provider = Provider
    module.Consumer = Consumer
    monkeypatch.setitem(sys.modules, module.__name__, module)
    provider = PluginDefinition(
        plugin_id="provider",
        version="1",
        api_version="1",
        entry_point=f"{module.__name__}:Provider",
        plugin_dependencies=(),
        service_dependencies=(),
        provided_services=(ProvidedService("clock", "1"),),
        capability_requests=(),
        config_schema=None,
        source=str(tmp_path / "provider.toml"),
    )
    consumer = PluginDefinition(
        plugin_id="consumer",
        version="1",
        api_version="1",
        entry_point=f"{module.__name__}:Consumer",
        plugin_dependencies=(),
        service_dependencies=(ServiceDependency("clock", ">=1", True),),
        provided_services=(),
        capability_requests=(),
        config_schema=None,
        source=str(tmp_path / "consumer.toml"),
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    definitions = (provider, consumer)
    host = PluginHost(definitions, build_activation_plan(definitions), config_path=config)

    async def scenario() -> None:
        await host.activate()
        handle = handles[0]
        saved_method = handle.service.now
        assert Implementation.accesses == 0
        running = asyncio.create_task(saved_method())
        await entered.wait()
        stopping = asyncio.create_task(host.deactivate("provider"))
        await asyncio.sleep(0)
        assert not stopping.done()
        assert refs[0]() is not None
        with pytest.raises(ServiceUnavailableError):
            await saved_method()
        release.set()
        assert await running == 7
        assert Implementation.accesses == 1
        await stopping
        gc.collect()
        assert refs[0]() is None
        assert handle.name == "clock"
        assert handle.provider_instance_id == "provider"
        with pytest.raises(ServiceUnavailableError):
            await saved_method()
        with pytest.raises(ServiceUnavailableError):
            await handle.service.now()
        await host.activate_instance("provider")
        with pytest.raises(ServiceUnavailableError):
            await saved_method()
        await host.dispose()

    asyncio.run(scenario())
