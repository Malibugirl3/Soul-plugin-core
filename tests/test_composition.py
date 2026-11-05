"""Static bundles preserve Core planning and preflight behavior."""

import asyncio
import sys
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from types import ModuleType

import pytest

from soul_plugin_core.builtin import memory_kv_manifest
from soul_plugin_core.builtin.key_value import KeyValueService
from soul_plugin_core.composition import PluginBundle, compose_plugins
from soul_plugin_core.errors import (
    CapabilityDeniedError,
    ConfigValidationError,
    DependencyResolutionError,
)
from soul_plugin_core.instances import InstanceBindingSelection, InstanceDefinition
from soul_plugin_core.services import ServiceHandle


def write_manifest(root: Path, name: str, body: str) -> Path:
    path = root / name / "plugin.toml"
    path.parent.mkdir()
    path.write_text(body, encoding="utf-8")
    return path


def manifest(plugin_id: str, entry: str, extra: str = "") -> str:
    return (
        "manifest_version = 1\n[plugin]\n"
        f'id = "{plugin_id}"\nversion = "1.0"\napi_version = "1"\n'
        f'entry_point = "{entry}"\n{extra}'
    )


def test_bundle_planning_is_deterministic_and_preserves_preflight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    (tmp_path / "composition_sentinel.py").write_text(
        "raise RuntimeError('plugin entry was imported')\n", encoding="utf-8"
    )
    provider = write_manifest(
        tmp_path,
        "provider",
        manifest(
            "provider",
            "composition_sentinel:Plugin",
            '[provides]\nservices = [{name="clock", version="1"}]\n',
        ),
    )
    consumer = write_manifest(
        tmp_path,
        "consumer",
        manifest(
            "consumer",
            "composition_sentinel:Plugin",
            '[dependencies]\nservices = [{name="clock", version=">=1"}]\n',
        ),
    )
    first = PluginBundle.from_manifests(consumer)
    second = PluginBundle.from_manifests(provider)
    missing_config = tmp_path / "missing.toml"
    a = compose_plugins(first, second, config_path=missing_config)
    b = compose_plugins(second, first, config_path=missing_config)
    assert a.plugin_plan == b.plugin_plan
    assert a.instance_plan == b.instance_plan
    assert a.instance_plan.order == ("provider", "consumer")
    assert "composition_sentinel" not in sys.modules
    host = a.create_host()

    async def scenario() -> None:
        with pytest.raises(ConfigValidationError):
            await host.activate()
        assert "composition_sentinel" not in sys.modules
        await host.dispose()

    asyncio.run(scenario())


def test_bundle_snapshots_inputs_and_explicit_empty_instances(tmp_path: Path) -> None:
    path = write_manifest(tmp_path, "plugin", manifest("plugin", "unused_module:Plugin"))
    paths = [path]
    bundle = PluginBundle(paths)
    paths.clear()
    assert bundle.manifests == (path,)
    with pytest.raises(FrozenInstanceError):
        bundle.manifests = ()  # type: ignore[misc]
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    assert compose_plugins(bundle, config_path=config).instance_plan.order == ("plugin",)
    empty = compose_plugins(bundle, config_path=config, instances=())
    assert empty.instance_plan.order == ()
    assert empty.instances == ()

    async def scenario() -> None:
        host = empty.create_host()
        await host.activate()
        assert "unused_module" not in sys.modules
        await host.dispose()

    asyncio.run(scenario())


def test_replacing_instances_recomputes_the_inspectable_plan(tmp_path: Path) -> None:
    path = write_manifest(tmp_path, "plugin", manifest("plugin", "unused_module:Plugin"))
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    original = compose_plugins(PluginBundle.from_manifests(path), config_path=config)
    changed = replace(original, instances=())
    assert original.instance_plan.order == ("plugin",)
    assert changed.instance_plan.order == ()

    async def scenario() -> None:
        host = changed.create_host()
        await host.activate()
        assert "unused_module" not in sys.modules
        await host.dispose()

    asyncio.run(scenario())


def test_composition_snapshots_mutable_binding_lists(
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

    module = ModuleType("bundle_mutable_consumer")
    module.Consumer = Consumer  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    path = write_manifest(
        tmp_path,
        "consumer",
        manifest(
            "consumer",
            "bundle_mutable_consumer:Consumer",
            '[dependencies]\nservices = [{name="core.kv", version=">=1"}]\n',
        ),
    )
    selections = [InstanceBindingSelection("service", "core.kv", "kv-a")]
    instances = (
        InstanceDefinition("kv-a", "core.memory_kv"),
        InstanceDefinition("kv-b", "core.memory_kv"),
        InstanceDefinition("client", "consumer", selections),  # type: ignore[arg-type]
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    composition = compose_plugins(
        PluginBundle.from_manifests(memory_kv_manifest(), path),
        config_path=config,
        instances=instances,
    )
    selections[0] = InstanceBindingSelection("service", "core.kv", "kv-b")
    assert composition.instance_plan.bindings[0].provider_instance_id == "kv-a"

    async def scenario() -> None:
        host = composition.create_host()
        await host.activate()
        assert handles[0].provider_instance_id == "kv-a"
        await host.dispose()

    asyncio.run(scenario())


def test_composition_rejects_duplicate_providers_before_loading_code(tmp_path: Path) -> None:
    extra = '[provides]\nservices = [{name="shared", version="1"}]\n'
    first = write_manifest(tmp_path, "first", manifest("first", "never_imported:Plugin", extra))
    second = write_manifest(tmp_path, "second", manifest("second", "never_imported:Plugin", extra))
    with pytest.raises(DependencyResolutionError):
        compose_plugins(
            PluginBundle.from_manifests(first, second), config_path=tmp_path / "missing"
        )
    assert "never_imported" not in sys.modules


def test_composition_exposes_unbound_optional_without_importing_entry(tmp_path: Path) -> None:
    path = write_manifest(
        tmp_path,
        "consumer",
        manifest(
            "consumer",
            "never_imported:Plugin",
            '[dependencies]\nservices = [{name="missing", version=">=1", optional=true}]\n',
        ),
    )
    plan = compose_plugins(PluginBundle.from_manifests(path), config_path=tmp_path / "missing")
    assert [(item.target, item.reason) for item in plan.plugin_plan.unbound] == [
        ("missing", "missing")
    ]
    assert [(item.target, item.reason) for item in plan.instance_plan.unbound] == [
        ("missing", "missing")
    ]
    assert "never_imported" not in sys.modules


def test_composition_denies_ungranted_capabilities_before_import(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    (tmp_path / "composition_denied.py").write_text(
        "raise RuntimeError('plugin entry was imported')\n", encoding="utf-8"
    )
    path = write_manifest(
        tmp_path,
        "plugin",
        manifest(
            "plugin",
            "composition_denied:Plugin",
            '[capabilities]\nrequests = ["events.publish"]\n',
        ),
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    host = compose_plugins(PluginBundle.from_manifests(path), config_path=config).create_host()

    async def scenario() -> None:
        with pytest.raises(CapabilityDeniedError):
            await host.activate()
        assert "composition_denied" not in sys.modules
        await host.dispose()

    asyncio.run(scenario())


def test_composition_snapshots_instance_generator_and_selected_binding(
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

    module = ModuleType("bundle_consumer")
    module.Consumer = Consumer  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    path = write_manifest(
        tmp_path,
        "consumer",
        manifest(
            "consumer",
            "bundle_consumer:Consumer",
            '[dependencies]\nservices = [{name="core.kv", version=">=1"}]\n',
        ),
    )
    declarations = (
        InstanceDefinition("kv-a", "core.memory_kv"),
        InstanceDefinition("kv-b", "core.memory_kv"),
        InstanceDefinition(
            "client", "consumer", (InstanceBindingSelection("service", "core.kv", "kv-b"),)
        ),
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    composition = compose_plugins(
        PluginBundle.from_manifests(memory_kv_manifest(), path),
        config_path=config,
        instances=(item for item in declarations),
    )
    assert composition.instances == declarations
    assert any(item.provider_instance_id == "kv-b" for item in composition.instance_plan.bindings)
    host = composition.create_host()

    async def scenario() -> None:
        await host.activate()
        assert handles[0].provider_instance_id == "kv-b"
        await handles[0].service.put("key", "value")
        assert await handles[0].service.get("key") == "value"
        await host.dispose()

    asyncio.run(scenario())
