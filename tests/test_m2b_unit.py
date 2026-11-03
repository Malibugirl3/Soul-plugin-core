"""M2b 内部行为：缺实例、配置隔离，以及沿实例计划的事务。"""

import asyncio
import sys
from pathlib import Path
from types import ModuleType

import pytest

from soul_plugin_core.config import validate_deployment
from soul_plugin_core.contracts import PluginDefinition, ProvidedService, ServiceDependency
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import (
    ConfigValidationError,
    InstanceResolutionError,
    PluginCleanupError,
    PluginLoadError,
)
from soul_plugin_core.host import PluginHost
from soul_plugin_core.instances import InstanceDefinition, expand_instance_plan
from soul_plugin_core.lifecycle import FiberState

SCHEMA = (
    '{"$schema":"https://json-schema.org/draft/2020-12/schema",'
    '"type":"object","properties":{"region":{"type":"string"}},'
    '"required":["region"],"additionalProperties":false}'
)


def run(awaitable):
    return asyncio.run(awaitable)


def definition(
    root: Path,
    plugin_id: str,
    module_name: str,
    *,
    services: tuple[ServiceDependency, ...] = (),
) -> PluginDefinition:
    plugin_dir = root / plugin_id
    plugin_dir.mkdir(parents=True, exist_ok=True)
    manifest = plugin_dir / "plugin.toml"
    manifest.write_text("# source marker\n", encoding="utf-8")
    (plugin_dir / "schema.json").write_text(SCHEMA, encoding="utf-8")
    return PluginDefinition(
        plugin_id=plugin_id,
        version="1.0",
        api_version="1",
        entry_point=f"{module_name}:Plugin",
        plugin_dependencies=(),
        service_dependencies=services,
        provided_services=(),
        capability_requests=(),
        config_schema="schema.json",
        source=str(manifest),
    )


def bare(
    plugin_id: str, *, services: tuple[ServiceDependency, ...] = (), provides: tuple[str, ...] = ()
):
    return PluginDefinition(
        plugin_id=plugin_id,
        version="1.0",
        api_version="1",
        entry_point="unused:Plugin",
        plugin_dependencies=(),
        service_dependencies=services,
        provided_services=tuple(ProvidedService(name, "1.0") for name in provides),
        capability_requests=(),
        config_schema=None,
        source=f"{plugin_id}/plugin.toml",
    )


def test_missing_provider_instance_fails_only_for_required_dependency() -> None:
    definitions = (
        bare("clock", provides=("clock",)),
        bare("consumer", services=(ServiceDependency("clock", ">=1", False),)),
        bare("optional-consumer", services=(ServiceDependency("clock", ">=1", True),)),
    )
    plan = build_activation_plan(definitions)
    with pytest.raises(InstanceResolutionError) as caught:
        expand_instance_plan(
            definitions,
            plan,
            (InstanceDefinition("consumer", "consumer"),),
        )
    assert caught.value.instance_id == "consumer"

    expanded = expand_instance_plan(
        definitions,
        plan,
        (InstanceDefinition("optional-consumer", "optional-consumer"),),
    )
    assert expanded.order == ("optional-consumer",)
    assert expanded.unbound[0].reason == "missing"
    assert expanded.unbound[0].consumer_instance_id == "optional-consumer"


def test_invalid_declaration_does_not_read_config(tmp_path: Path) -> None:
    item = definition(tmp_path, "demo", "unused")
    missing = tmp_path / "missing.toml"
    definitions = (item,)
    with pytest.raises(InstanceResolutionError):
        PluginHost(
            definitions,
            build_activation_plan(definitions),
            config_path=missing,
            instances=(InstanceDefinition(" ", "demo"),),
        )
    assert not missing.exists()


def test_two_instances_keep_separate_objects_config_and_stop_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    events = tmp_path / "events.txt"
    (tmp_path / "m2b_worker.py").write_text(
        "from pathlib import Path\n"
        f"EVENTS = Path({str(events)!r})\n"
        "SEEN = []\n"
        "def emit(value):\n"
        "    with EVENTS.open('a', encoding='utf-8') as stream:\n"
        "        stream.write(value + '\\n')\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        SEEN.append((context.instance_id, id(self), config['region'], context.config is config))\n"
        "        def cleanup():\n"
        "            emit(context.instance_id + ':cleanup')\n"
        "        context.effects.defer(cleanup, label='cleanup')\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        emit(self_id(self) + ':stop')\n"
        "def self_id(plugin):\n"
        "    return next(item[0] for item in SEEN if item[1] == id(plugin))\n",
        encoding="utf-8",
    )
    item = definition(tmp_path, "worker", "m2b_worker")
    config = tmp_path / "config.toml"
    config.write_text(
        '[plugins.worker-eu]\nregion = "eu"\n[plugins.worker-us]\nregion = "us"\n',
        encoding="utf-8",
    )
    definitions = (item,)
    host = PluginHost(
        definitions,
        build_activation_plan(definitions),
        config_path=config,
        instances=(
            InstanceDefinition("worker-us", "worker"),
            InstanceDefinition("worker-eu", "worker"),
        ),
    )
    run(host.activate())
    seen = sys.modules["m2b_worker"].SEEN
    assert [item[0] for item in seen] == ["worker-eu", "worker-us"]
    assert seen[0][1] != seen[1][1]
    assert [item[2] for item in seen] == ["eu", "us"]
    assert all(item[3] for item in seen)
    run(host.stop())
    assert events.read_text(encoding="utf-8").splitlines() == [
        "worker-us:stop",
        "worker-us:cleanup",
        "worker-eu:stop",
        "worker-eu:cleanup",
    ]
    assert host.get_state("worker-eu") is FiberState.PENDING
    assert host.get_state("worker-us") is FiberState.PENDING


def test_cancellation_rolls_back_only_instances_started_in_this_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    events = tmp_path / "events.txt"
    (tmp_path / "m2b_cancel.py").write_text(
        "import asyncio\n"
        "import m2b_cancel_ready\n"
        "from pathlib import Path\n"
        f"EVENTS = Path({str(events)!r})\n"
        "def emit(value):\n"
        "    with EVENTS.open('a', encoding='utf-8') as stream:\n"
        "        stream.write(value + '\\n')\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        self.instance_id = context.instance_id\n"
        "        def cleanup():\n"
        "            emit(self.instance_id + ':cleanup')\n"
        "        context.effects.defer(cleanup, label='cleanup')\n"
        "    async def start(self):\n"
        "        if self.instance_id == 'worker-us':\n"
        "            m2b_cancel_ready.ready.set()\n"
        "            await asyncio.sleep(30)\n"
        "    async def stop(self):\n"
        "        emit(self.instance_id + ':stop')\n",
        encoding="utf-8",
    )
    item = definition(tmp_path, "worker", "m2b_cancel")
    config = tmp_path / "config.toml"
    config.write_text(
        '[plugins.worker-eu]\nregion = "eu"\n[plugins.worker-us]\nregion = "us"\n',
        encoding="utf-8",
    )
    definitions = (item,)
    host = PluginHost(
        definitions,
        build_activation_plan(definitions),
        config_path=config,
        instances=(
            InstanceDefinition("worker-eu", "worker"),
            InstanceDefinition("worker-us", "worker"),
        ),
    )

    async def main() -> None:
        support = ModuleType("m2b_cancel_ready")
        ready = asyncio.Event()
        support.ready = ready
        sys.modules["m2b_cancel_ready"] = support
        task = asyncio.create_task(host.activate())
        await asyncio.wait_for(ready.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    try:
        run(main())
    finally:
        sys.modules.pop("m2b_cancel_ready", None)
        sys.modules.pop("m2b_cancel", None)
    assert host.get_state("worker-eu") is FiberState.PENDING
    assert host.get_state("worker-us") is FiberState.FAILED
    assert host._fibers["worker-eu"].scope is None
    assert host._fibers["worker-us"].scope is None
    assert events.read_text(encoding="utf-8").splitlines() == [
        "worker-us:stop",
        "worker-us:cleanup",
        "worker-eu:stop",
        "worker-eu:cleanup",
    ]


def test_dispose_failure_still_disposes_every_instance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    events = tmp_path / "events.txt"
    (tmp_path / "m2b_dispose.py").write_text(
        "from pathlib import Path\n"
        f"EVENTS = Path({str(events)!r})\n"
        "def emit(value):\n"
        "    with EVENTS.open('a', encoding='utf-8') as stream:\n"
        "        stream.write(value + '\\n')\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        self.instance_id = context.instance_id\n"
        "        def cleanup():\n"
        "            emit(self.instance_id + ':cleanup')\n"
        "        context.effects.defer(cleanup, label='cleanup')\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        emit(self.instance_id + ':stop')\n"
        "        if self.instance_id == 'worker-us':\n"
        "            raise RuntimeError('stop failed')\n",
        encoding="utf-8",
    )
    item = definition(tmp_path, "worker", "m2b_dispose")
    config = tmp_path / "config.toml"
    config.write_text(
        '[plugins.worker-eu]\nregion = "eu"\n[plugins.worker-us]\nregion = "us"\n',
        encoding="utf-8",
    )
    definitions = (item,)
    host = PluginHost(
        definitions,
        build_activation_plan(definitions),
        config_path=config,
        instances=(
            InstanceDefinition("worker-eu", "worker"),
            InstanceDefinition("worker-us", "worker"),
        ),
    )
    run(host.activate())
    with pytest.raises(PluginCleanupError):
        run(host.dispose())
    assert host.get_state("worker-eu") is FiberState.DISPOSED
    assert host.get_state("worker-us") is FiberState.DISPOSED
    recorded = events.read_text(encoding="utf-8")
    run(host.dispose())
    assert events.read_text(encoding="utf-8") == recorded


def test_construct_failure_names_the_failed_instance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    (tmp_path / "m2b_construct.py").write_text(
        "COUNT = 0\n"
        "class Plugin:\n"
        "    def __init__(self):\n"
        "        global COUNT\n"
        "        COUNT += 1\n"
        "        if COUNT == 2:\n"
        "            raise RuntimeError('construct failed')\n"
        "    async def setup(self, context, config):\n"
        "        return None\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
        encoding="utf-8",
    )
    item = definition(tmp_path, "worker", "m2b_construct")
    config = tmp_path / "config.toml"
    config.write_text(
        '[plugins.worker-eu]\nregion = "eu"\n[plugins.worker-us]\nregion = "us"\n',
        encoding="utf-8",
    )
    definitions = (item,)
    host = PluginHost(
        definitions,
        build_activation_plan(definitions),
        config_path=config,
        instances=(
            InstanceDefinition("worker-eu", "worker"),
            InstanceDefinition("worker-us", "worker"),
        ),
    )
    with pytest.raises(PluginLoadError) as caught:
        run(host.activate())
    assert caught.value.plugin_id == "worker"
    assert caught.value.instance_id == "worker-us"
    assert host.get_state("worker-us") is FiberState.FAILED


def test_non_table_config_keeps_plugin_and_instance_identity(tmp_path: Path) -> None:
    item = definition(tmp_path, "worker", "unused")
    config = tmp_path / "config.toml"
    config.write_text("[plugins]\nworker-eu = 1\n", encoding="utf-8")
    with pytest.raises(ConfigValidationError) as caught:
        validate_deployment({"worker-eu": item, "worker-us": item}, config)
    assert caught.value.plugin_id == "worker"
    assert caught.value.instance_id == "worker-eu"
