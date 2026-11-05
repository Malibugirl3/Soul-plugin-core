"""The bundled memory service is usable through the regular plugin contract."""

import asyncio
import gc
import sys
import weakref
from pathlib import Path
from types import ModuleType

import pytest

from soul_plugin_core.builtin import memory_kv_manifest
from soul_plugin_core.builtin.key_value import KeyValueService
from soul_plugin_core.contracts import PluginDefinition, ServiceDependency
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import ConfigValidationError, ServiceUnavailableError
from soul_plugin_core.host import PluginHost
from soul_plugin_core.instances import InstanceBindingSelection, InstanceDefinition
from soul_plugin_core.manifest import load_manifest
from soul_plugin_core.services import ServiceHandle


def test_memory_kv_instances_are_isolated_and_restart_with_fresh_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handles: dict[str, ServiceHandle[KeyValueService]] = {}

    class RetainedValue(str):
        pass

    class Consumer:
        async def setup(self, context, config):
            handles[context.instance_id] = context.services.require("core.kv", KeyValueService)

        async def start(self):
            pass

        async def stop(self):
            pass

    module = ModuleType("builtin_kv_consumer")
    module.Consumer = Consumer  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    provider = load_manifest(memory_kv_manifest())
    consumer = PluginDefinition(
        plugin_id="kv-consumer",
        version="1.0",
        api_version="1",
        entry_point="builtin_kv_consumer:Consumer",
        plugin_dependencies=(),
        service_dependencies=(ServiceDependency("core.kv", ">=1,<2", False),),
        provided_services=(),
        capability_requests=(),
        config_schema=None,
        source=str(tmp_path / "consumer.toml"),
    )
    definitions = (provider, consumer)
    instances = (
        InstanceDefinition("kv-a", provider.plugin_id),
        InstanceDefinition("kv-b", provider.plugin_id),
        InstanceDefinition(
            "consumer-a", "kv-consumer", (InstanceBindingSelection("service", "core.kv", "kv-a"),)
        ),
        InstanceDefinition(
            "consumer-b", "kv-consumer", (InstanceBindingSelection("service", "core.kv", "kv-b"),)
        ),
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    host = PluginHost(
        definitions, build_activation_plan(definitions), config_path=config, instances=instances
    )

    async def scenario() -> None:
        await host.activate()
        a, b = handles["consumer-a"], handles["consumer-b"]
        assert (a.provider_instance_id, b.provider_instance_id) == ("kv-a", "kv-b")
        assert await a.service.get("key") is None
        await a.service.put("key", "A")
        await b.service.put("key", "B")
        assert await a.service.get("key") == "A"
        assert await b.service.get("key") == "B"
        assert await a.service.delete("key") is True
        assert await a.service.delete("key") is False
        assert await b.service.get("key") == "B"
        value = RetainedValue("held by store")
        reference = weakref.ref(value)
        await b.service.put("retained", value)
        del value
        assert reference() is not None
        await host.stop()
        gc.collect()
        assert reference() is None
        with pytest.raises(ServiceUnavailableError):
            await b.service.get("key")
        await host.activate()
        assert handles["consumer-b"].generation > b.generation
        assert await handles["consumer-b"].service.get("key") is None
        with pytest.raises(ServiceUnavailableError):
            await b.service.get("key")
        await host.dispose()

    asyncio.run(scenario())


def test_bad_config_does_not_import_memory_plugin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module_name = "soul_plugin_core.builtin.memory_kv"
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    provider = load_manifest(memory_kv_manifest())
    config = tmp_path / "config.toml"
    config.write_text('[plugins."core.memory_kv"]\nvalue = 1\n', encoding="utf-8")
    host = PluginHost((provider,), build_activation_plan((provider,)), config_path=config)

    async def scenario() -> None:
        with pytest.raises(ConfigValidationError):
            await host.activate()
        assert module_name not in sys.modules
        await host.dispose()

    try:
        asyncio.run(scenario())
    finally:
        sys.modules.pop(module_name, None)


def test_memory_kv_rejects_non_string_keys_and_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handles: list[ServiceHandle[KeyValueService]] = []

    class Consumer:
        async def setup(self, context, config):
            handles.append(context.services.require("core.kv", KeyValueService))

        async def start(self):
            pass

        async def stop(self):
            pass

    module = ModuleType("builtin_kv_consumer")
    module.Consumer = Consumer  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    provider = load_manifest(memory_kv_manifest())
    consumer = PluginDefinition(
        plugin_id="kv-consumer",
        version="1.0",
        api_version="1",
        entry_point="builtin_kv_consumer:Consumer",
        plugin_dependencies=(),
        service_dependencies=(ServiceDependency("core.kv", ">=1,<2", False),),
        provided_services=(),
        capability_requests=(),
        config_schema=None,
        source=str(tmp_path / "consumer.toml"),
    )
    definitions = (provider, consumer)
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    host = PluginHost(definitions, build_activation_plan(definitions), config_path=config)

    async def scenario() -> None:
        await host.activate()
        with pytest.raises(TypeError):
            await handles[0].service.get(1)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            await handles[0].service.put("key", 1)  # type: ignore[arg-type]
        assert await handles[0].service.get("key") is None
        await host.dispose()

    asyncio.run(scenario())
