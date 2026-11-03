"""M3a service acceptance tests. The service module is implemented separately."""

import asyncio
import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from soul_plugin_core.contracts import PluginDefinition, ProvidedService, ServiceDependency
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import PluginActivationError, PluginLoadError
from soul_plugin_core.host import PluginHost
from soul_plugin_core.instances import InstanceBindingSelection, InstanceDefinition
from soul_plugin_core.lifecycle import FiberState


@pytest.fixture
def m3_api() -> SimpleNamespace:
    services = importlib.import_module("soul_plugin_core.services")
    errors = importlib.import_module("soul_plugin_core.errors")
    return SimpleNamespace(
        ServiceHandle=services.ServiceHandle,
        ServiceUnavailableError=errors.ServiceUnavailableError,
    )


def run(awaitable):
    return asyncio.run(awaitable)


def definition(
    root: Path,
    plugin_id: str,
    module_name: str,
    *,
    services: tuple[ServiceDependency, ...] = (),
    provides: tuple[ProvidedService, ...] = (),
) -> PluginDefinition:
    plugin_dir = root / plugin_id
    plugin_dir.mkdir(parents=True, exist_ok=True)
    manifest = plugin_dir / "plugin.toml"
    manifest.write_text("# source marker\n", encoding="utf-8")
    return PluginDefinition(
        plugin_id=plugin_id,
        version="1.0",
        api_version="1",
        entry_point=f"{module_name}:Plugin",
        plugin_dependencies=(),
        service_dependencies=services,
        provided_services=provides,
        capability_requests=(),
        config_schema=None,
        source=str(manifest),
    )


def write_config(root: Path, instance_ids: tuple[str, ...]) -> Path:
    path = root / "config.toml"
    path.write_text(
        "".join(f"[plugins.{instance_id}]\n" for instance_id in instance_ids), encoding="utf-8"
    )
    return path


def write_module(root: Path, module_name: str, source: str) -> None:
    (root / f"{module_name}.py").write_text(source, encoding="utf-8")


def unload(*module_names: str) -> None:
    for module_name in module_names:
        sys.modules.pop(module_name, None)


def events(path: Path) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8").splitlines()


def header(event_path: Path) -> str:
    return (
        "from pathlib import Path\n"
        f"EVENTS = Path({str(event_path)!r})\n"
        "def emit(value):\n"
        "    with EVENTS.open('a', encoding='utf-8') as stream:\n"
        "        stream.write(value + '\\n')\n"
    )


def clock_protocol() -> str:
    return (
        "from typing import Protocol\nclass Clock(Protocol):\n    async def now(self) -> int: ...\n"
    )


def make_host(definitions, config_path: Path, instances=None) -> PluginHost:
    stored = tuple(definitions)
    return PluginHost(
        stored,
        build_activation_plan(stored),
        config_path=config_path,
        instances=instances,
    )


def test_committed_service_is_available_in_consumer_setup(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(
        tmp_path,
        "m3_commit_clock",
        header(event_path)
        + "PROVIDED = None\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global PROVIDED\n"
        + "        PROVIDED = self\n"
        + "        emit('clock:setup')\n"
        + "        context.services.provide('clock', self)\n"
        + "    async def start(self):\n"
        + "        emit('clock:start')\n"
        + "    async def stop(self):\n"
        + "        emit('clock:stop')\n"
        + "    async def now(self):\n"
        + "        return 7\n",
    )
    write_module(
        tmp_path,
        "m3_commit_consumer",
        header(event_path)
        + clock_protocol()
        + "HANDLE = None\n"
        + "VALUE = None\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global HANDLE, VALUE\n"
        + "        emit('consumer:setup')\n"
        + "        HANDLE = context.services.require('clock', Clock)\n"
        + "        VALUE = await HANDLE.service.now()\n"
        + "        for banned in ('get', 'list', 'lookup', 'registry'):\n"
        + "            if hasattr(context.services, banned):\n"
        + "                raise AssertionError(banned)\n"
        + "    async def start(self):\n"
        + "        emit('consumer:start')\n"
        + "    async def stop(self):\n"
        + "        emit('consumer:stop')\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        "m3_commit_clock",
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    consumer = definition(
        tmp_path,
        "consumer",
        "m3_commit_consumer",
        services=(ServiceDependency("clock", ">=1", False),),
    )
    host = make_host((clock, consumer), write_config(tmp_path, ("clock", "consumer")))
    try:
        run(host.activate())
        handle = sys.modules["m3_commit_consumer"].HANDLE
        provided = sys.modules["m3_commit_clock"].PROVIDED
        assert events(event_path)[:4] == [
            "clock:setup",
            "clock:start",
            "consumer:setup",
            "consumer:start",
        ]
        assert sys.modules["m3_commit_consumer"].VALUE == 7
        assert isinstance(handle, m3_api.ServiceHandle)
        assert handle.service is not provided
        assert handle.name == "clock"
        assert handle.version == "1.4.0"
        assert handle.provider_plugin_id == "clock"
        assert handle.provider_instance_id == "clock"
        assert isinstance(handle.generation, int)
        assert run(handle.service.now()) == 7
    finally:
        unload("m3_commit_clock", "m3_commit_consumer")


def test_business_exception_is_not_wrapped(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3_business_clock",
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        context.services.provide('clock', self)\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n"
        "    async def now(self):\n"
        "        raise RuntimeError('sensor offline')\n",
    )
    write_module(
        tmp_path,
        "m3_business_consumer",
        clock_protocol()
        + "HANDLE = None\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global HANDLE\n"
        + "        HANDLE = context.services.require('clock', Clock)\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        return None\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        "m3_business_clock",
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    consumer = definition(
        tmp_path,
        "consumer",
        "m3_business_consumer",
        services=(ServiceDependency("clock", ">=1", False),),
    )
    host = make_host((clock, consumer), write_config(tmp_path, ("clock", "consumer")))
    try:
        run(host.activate())
        handle = sys.modules["m3_business_consumer"].HANDLE
        with pytest.raises(RuntimeError, match="sensor offline") as caught:
            run(handle.service.now())
        assert not isinstance(caught.value, m3_api.ServiceUnavailableError)
    finally:
        unload("m3_business_clock", "m3_business_consumer")


def test_proxy_sync_method_is_not_invoked(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3_sync_clock",
        "INSTANCE = None\n"
        "class Plugin:\n"
        "    def __init__(self):\n"
        "        self.sync_calls = 0\n"
        "    async def setup(self, context, config):\n"
        "        global INSTANCE\n"
        "        INSTANCE = self\n"
        "        context.services.provide('clock', self)\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n"
        "    async def now(self):\n"
        "        return 1\n"
        "    def sync_tick(self):\n"
        "        self.sync_calls += 1\n",
    )
    write_module(
        tmp_path,
        "m3_sync_consumer",
        clock_protocol()
        + "HANDLE = None\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global HANDLE\n"
        + "        HANDLE = context.services.require('clock', Clock)\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        return None\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        "m3_sync_clock",
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    consumer = definition(
        tmp_path,
        "consumer",
        "m3_sync_consumer",
        services=(ServiceDependency("clock", ">=1", False),),
    )
    host = make_host((clock, consumer), write_config(tmp_path, ("clock", "consumer")))
    try:
        run(host.activate())
        handle = sys.modules["m3_sync_consumer"].HANDLE
        with pytest.raises(AttributeError):
            handle.service.sync_tick()
        with pytest.raises(AttributeError):
            _ = handle.service.missing
        assert sys.modules["m3_sync_clock"].INSTANCE.sync_calls == 0
    finally:
        unload("m3_sync_clock", "m3_sync_consumer")


@pytest.mark.parametrize("surface", ["property", "getattr"])
def test_proxy_does_not_execute_descriptor_on_lookup(m3_api, tmp_path, monkeypatch, surface: str):
    monkeypatch.syspath_prepend(str(tmp_path))
    provider_module = f"m3_descriptor_{surface}"
    consumer_module = f"m3_descriptor_{surface}_consumer"
    if surface == "property":
        surface_source = (
            "    @property\n    def now(self):\n        self.reads += 1\n        return None\n"
        )
    else:
        surface_source = (
            "    def __getattr__(self, name):\n"
            "        if name == 'now':\n"
            "            self.reads += 1\n"
            "            async def hidden():\n"
            "                return 1\n"
            "            return hidden\n"
            "        raise AttributeError(name)\n"
        )
    write_module(
        tmp_path,
        provider_module,
        "INSTANCE = None\n"
        "class Plugin:\n"
        "    def __init__(self):\n"
        "        self.reads = 0\n"
        "    async def setup(self, context, config):\n"
        "        global INSTANCE\n"
        "        INSTANCE = self\n"
        "        context.services.provide('clock', self)\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n" + surface_source,
    )
    write_module(
        tmp_path,
        consumer_module,
        clock_protocol()
        + "HANDLE = None\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global HANDLE\n"
        + "        HANDLE = context.services.require('clock', Clock)\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        return None\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        provider_module,
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    consumer = definition(
        tmp_path,
        "consumer",
        consumer_module,
        services=(ServiceDependency("clock", ">=1", False),),
    )
    host = make_host((clock, consumer), write_config(tmp_path, ("clock", "consumer")))
    try:
        run(host.activate())
        handle = sys.modules[consumer_module].HANDLE
        with pytest.raises(AttributeError):
            _ = handle.service.now
        assert sys.modules[provider_module].INSTANCE.reads == 0
    finally:
        unload(provider_module, consumer_module)


@pytest.mark.parametrize(
    "mode",
    ["undeclared", "duplicate"],
)
def test_bad_provide_fails_in_setup_without_stop(m3_api, tmp_path, monkeypatch, mode):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    second = "        context.services.provide('clock', self)\n" if mode == "duplicate" else ""
    name = "other" if mode == "undeclared" else "clock"
    write_module(
        tmp_path,
        "m3_bad_provide",
        header(event_path)
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        emit('clock:setup')\n"
        + f"        context.services.provide({name!r}, self)\n"
        + second
        + "    async def start(self):\n"
        + "        emit('clock:start')\n"
        + "    async def stop(self):\n"
        + "        emit('clock:stop')\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        "m3_bad_provide",
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    host = make_host((clock,), write_config(tmp_path, ("clock",)))
    try:
        with pytest.raises(PluginActivationError) as caught:
            run(host.activate())
        assert caught.value.plugin_id == "clock"
        assert caught.value.instance_id == "clock"
        assert "clock:stop" not in events(event_path)
        assert host.get_state("clock") is FiberState.FAILED
    finally:
        unload("m3_bad_provide")


@pytest.mark.parametrize("call", ["provide", "require", "optional"])
def test_service_call_outside_setup_stops_after_successful_setup(
    m3_api, tmp_path, monkeypatch, call: str
):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    module_name = f"m3_late_{call}"
    protocol = "" if call == "provide" else clock_protocol()
    if call == "provide":
        invocation = "        context.services.provide('clock', self)\n"
    else:
        invocation = f"        context.services.{call}('clock', Clock)\n"
    write_module(
        tmp_path,
        module_name,
        header(event_path)
        + protocol
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        emit('clock:setup')\n"
        + "        context.effects.defer(lambda: emit('clock:cleanup'))\n"
        + "    async def start(self):\n"
        + "        emit('clock:start')\n"
        + invocation
        + "    async def stop(self):\n"
        + "        emit('clock:stop')\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        module_name,
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    host = make_host((clock,), write_config(tmp_path, ("clock",)))
    try:
        with pytest.raises(PluginActivationError) as caught:
            run(host.activate())
        assert caught.value.plugin_id == "clock"
        assert caught.value.instance_id == "clock"
        assert events(event_path) == [
            "clock:setup",
            "clock:start",
            "clock:stop",
            "clock:cleanup",
        ]
        assert host.get_state("clock") is FiberState.FAILED
    finally:
        unload(module_name)


def test_missing_declared_service_stops_after_start_and_hides_consumer(
    m3_api, tmp_path, monkeypatch
):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(
        tmp_path,
        "m3_missing_clock",
        header(event_path)
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        emit('clock:setup')\n"
        + "    async def start(self):\n"
        + "        emit('clock:start')\n"
        + "    async def stop(self):\n"
        + "        emit('clock:stop')\n",
    )
    write_module(
        tmp_path,
        "m3_missing_consumer",
        header(event_path)
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        emit('consumer:setup')\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        return None\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        "m3_missing_clock",
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    consumer = definition(
        tmp_path,
        "consumer",
        "m3_missing_consumer",
        services=(ServiceDependency("clock", ">=1", False),),
    )
    host = make_host((clock, consumer), write_config(tmp_path, ("clock", "consumer")))
    try:
        with pytest.raises(PluginActivationError) as caught:
            run(host.activate())
        assert caught.value.instance_id == "clock"
        assert "consumer:setup" not in events(event_path)
        assert events(event_path) == ["clock:setup", "clock:start", "clock:stop"]
        assert host.get_state("clock") is FiberState.FAILED
    finally:
        unload("m3_missing_clock", "m3_missing_consumer")


def test_start_failure_discards_staged_service(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(
        tmp_path,
        "m3_staged_clock",
        header(event_path)
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        emit('clock:setup')\n"
        + "        context.services.provide('clock', self)\n"
        + "    async def start(self):\n"
        + "        emit('clock:start')\n"
        + "        raise RuntimeError('start failed')\n"
        + "    async def stop(self):\n"
        + "        emit('clock:stop')\n"
        + "    async def now(self):\n"
        + "        return 1\n",
    )
    write_module(
        tmp_path,
        "m3_staged_consumer",
        header(event_path)
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        emit('consumer:setup')\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        return None\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        "m3_staged_clock",
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    consumer = definition(
        tmp_path,
        "consumer",
        "m3_staged_consumer",
        services=(ServiceDependency("clock", ">=1", False),),
    )
    host = make_host((clock, consumer), write_config(tmp_path, ("clock", "consumer")))
    try:
        with pytest.raises(PluginActivationError):
            run(host.activate())
        assert "consumer:setup" not in events(event_path)
        assert events(event_path) == ["clock:setup", "clock:start", "clock:stop"]
    finally:
        unload("m3_staged_clock", "m3_staged_consumer")


@pytest.mark.parametrize(
    ("dependency_optional", "call", "service_name"),
    [
        (True, "require", "clock"),
        (False, "optional", "clock"),
        (False, "require", "other"),
    ],
)
def test_require_and_optional_follow_the_declared_binding(
    m3_api, tmp_path, monkeypatch, dependency_optional, call, service_name
):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(
        tmp_path,
        "m3_binding_clock",
        header(event_path)
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        context.services.provide('clock', self)\n"
        + "    async def start(self):\n"
        + "        emit('clock:start')\n"
        + "    async def stop(self):\n"
        + "        emit('clock:stop')\n"
        + "    async def now(self):\n"
        + "        return 1\n",
    )
    write_module(
        tmp_path,
        "m3_binding_consumer",
        header(event_path)
        + clock_protocol()
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        emit('consumer:setup')\n"
        + f"        context.services.{call}({service_name!r}, Clock)\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        emit('consumer:stop')\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        "m3_binding_clock",
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    consumer = definition(
        tmp_path,
        "consumer",
        "m3_binding_consumer",
        services=(ServiceDependency("clock", ">=1", dependency_optional),),
    )
    host = make_host((clock, consumer), write_config(tmp_path, ("clock", "consumer")))
    try:
        with pytest.raises(PluginActivationError) as caught:
            run(host.activate())
        assert caught.value.plugin_id == "consumer"
        assert caught.value.instance_id == "consumer"
        assert "consumer:stop" not in events(event_path)
        assert "clock:stop" in events(event_path)
        assert host.get_state("clock") is FiberState.PENDING
        assert host.get_state("consumer") is FiberState.FAILED
    finally:
        unload("m3_binding_clock", "m3_binding_consumer")


def test_unbound_optional_returns_none(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3_unbound_consumer",
        clock_protocol()
        + "RESULT = 'unset'\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global RESULT\n"
        + "        RESULT = context.services.optional('clock', Clock)\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        return None\n",
    )
    consumer = definition(
        tmp_path,
        "consumer",
        "m3_unbound_consumer",
        services=(ServiceDependency("clock", ">=1", True),),
    )
    host = make_host((consumer,), write_config(tmp_path, ("consumer",)))
    try:
        run(host.activate())
        assert sys.modules["m3_unbound_consumer"].RESULT is None
        assert host.get_state("consumer") is FiberState.ACTIVE
    finally:
        unload("m3_unbound_consumer")


def test_bound_optional_returns_a_handle(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3_opt_clock",
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        context.services.provide('clock', self)\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n"
        "    async def now(self):\n"
        "        return 4\n",
    )
    write_module(
        tmp_path,
        "m3_opt_consumer",
        clock_protocol()
        + "HANDLE = None\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global HANDLE\n"
        + "        HANDLE = context.services.optional('clock', Clock)\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        return None\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        "m3_opt_clock",
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    consumer = definition(
        tmp_path,
        "consumer",
        "m3_opt_consumer",
        services=(ServiceDependency("clock", ">=1", True),),
    )
    host = make_host((clock, consumer), write_config(tmp_path, ("clock", "consumer")))
    try:
        run(host.activate())
        handle = sys.modules["m3_opt_consumer"].HANDLE
        assert isinstance(handle, m3_api.ServiceHandle)
        assert run(handle.service.now()) == 4
    finally:
        unload("m3_opt_clock", "m3_opt_consumer")


def test_all_declared_services_become_visible_together(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3_pair_provider",
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        context.services.provide('clock', self)\n"
        "        context.services.provide('meter', self)\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n"
        "    async def now(self):\n"
        "        return 3\n"
        "    async def read(self):\n"
        "        return 5\n",
    )
    write_module(
        tmp_path,
        "m3_pair_consumer",
        "from typing import Protocol\n"
        "class Clock(Protocol):\n"
        "    async def now(self) -> int: ...\n"
        "class Meter(Protocol):\n"
        "    async def read(self) -> int: ...\n"
        "VALUES = None\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        global VALUES\n"
        "        clock = context.services.require('clock', Clock)\n"
        "        meter = context.services.require('meter', Meter)\n"
        "        VALUES = (await clock.service.now(), await meter.service.read())\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    provider = definition(
        tmp_path,
        "provider",
        "m3_pair_provider",
        provides=(ProvidedService("clock", "1.4.0"), ProvidedService("meter", "2.0.0")),
    )
    consumer = definition(
        tmp_path,
        "consumer",
        "m3_pair_consumer",
        services=(
            ServiceDependency("clock", ">=1", False),
            ServiceDependency("meter", ">=1", False),
        ),
    )
    host = make_host((provider, consumer), write_config(tmp_path, ("provider", "consumer")))
    try:
        run(host.activate())
        assert sys.modules["m3_pair_consumer"].VALUES == (3, 5)
    finally:
        unload("m3_pair_provider", "m3_pair_consumer")


def test_selected_provider_instance_is_the_only_callee(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3_select_clock",
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        self.instance_id = context.instance_id\n"
        "        self.calls = 0\n"
        "        context.services.provide('clock', self)\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n"
        "    async def now(self):\n"
        "        self.calls += 1\n"
        "        return self.instance_id\n",
    )
    write_module(
        tmp_path,
        "m3_select_worker",
        clock_protocol()
        + "HANDLE = None\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global HANDLE\n"
        + "        HANDLE = context.services.require('clock', Clock)\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        return None\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        "m3_select_clock",
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    worker = definition(
        tmp_path,
        "worker",
        "m3_select_worker",
        services=(ServiceDependency("clock", ">=1", False),),
    )
    instances = (
        InstanceDefinition("clock-us", "clock"),
        InstanceDefinition(
            "worker",
            "worker",
            (InstanceBindingSelection("service", "clock", "clock-eu"),),
        ),
        InstanceDefinition("clock-eu", "clock"),
    )
    host = make_host(
        (clock, worker),
        write_config(tmp_path, ("clock-eu", "clock-us", "worker")),
        instances,
    )
    try:
        run(host.activate())
        handle = sys.modules["m3_select_worker"].HANDLE
        assert handle.provider_instance_id == "clock-eu"
        assert handle.provider_plugin_id == "clock"
        assert run(handle.service.now()) == "clock-eu"
    finally:
        unload("m3_select_clock", "m3_select_worker")


def test_same_call_consumer_failure_revokes_new_provider(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(
        tmp_path,
        "m3_rollback_clock",
        header(event_path)
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        context.services.provide('clock', self)\n"
        + "    async def start(self):\n"
        + "        emit('clock:start')\n"
        + "    async def stop(self):\n"
        + "        emit('clock:stop')\n"
        + "    async def now(self):\n"
        + "        return 1\n",
    )
    write_module(
        tmp_path,
        "m3_rollback_consumer",
        header(event_path)
        + clock_protocol()
        + "HANDLE = None\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global HANDLE\n"
        + "        HANDLE = context.services.require('clock', Clock)\n"
        + "        raise RuntimeError('consumer failed')\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        emit('consumer:stop')\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        "m3_rollback_clock",
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    consumer = definition(
        tmp_path,
        "consumer",
        "m3_rollback_consumer",
        services=(ServiceDependency("clock", ">=1", False),),
    )
    host = make_host((clock, consumer), write_config(tmp_path, ("clock", "consumer")))
    try:
        with pytest.raises(PluginActivationError):
            run(host.activate())
        handle = sys.modules["m3_rollback_consumer"].HANDLE
        assert "consumer:stop" not in events(event_path)
        assert "clock:stop" in events(event_path)
        assert host.get_state("clock") is FiberState.PENDING
        assert host.get_state("consumer") is FiberState.FAILED
        with pytest.raises(m3_api.ServiceUnavailableError) as caught:
            run(handle.service.now())
        assert caught.value.name == "clock"
        assert caught.value.provider_plugin_id == "clock"
        assert caught.value.provider_instance_id == "clock"
        assert caught.value.generation == handle.generation
    finally:
        unload("m3_rollback_clock", "m3_rollback_consumer")


def test_existing_provider_stays_active_when_another_consumer_fails(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3_keep_clock",
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        context.services.provide('clock', self)\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n"
        "    async def now(self):\n"
        "        return 9\n",
    )
    consumer_source = (
        clock_protocol()
        + "HANDLE = None\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global HANDLE\n"
        + "        HANDLE = context.services.require('clock', Clock)\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        return None\n"
    )
    write_module(tmp_path, "m3_keep_a", consumer_source)
    write_module(tmp_path, "m3_keep_b", consumer_source)
    clock = definition(
        tmp_path,
        "clock",
        "m3_keep_clock",
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    first = definition(
        tmp_path,
        "consumer-a",
        "m3_keep_a",
        services=(ServiceDependency("clock", ">=1", False),),
    )
    second = definition(
        tmp_path,
        "consumer-b",
        "m3_keep_b",
        services=(ServiceDependency("clock", ">=1", False),),
    )
    host = make_host(
        (clock, first, second),
        write_config(tmp_path, ("clock", "consumer-a", "consumer-b")),
    )
    try:
        run(host.activate())
        kept = sys.modules["m3_keep_a"].HANDLE
        first_handle = sys.modules["m3_keep_b"].HANDLE
        run(host._stop_fiber(host._fibers["consumer-b"], FiberState.PENDING))
        assert host.get_state("consumer-b") is FiberState.PENDING
        assert host._fibers["consumer-b"].scope is None
        with pytest.raises(m3_api.ServiceUnavailableError):
            run(first_handle.service.now())
        released = {}

        async def broken_setup(self, context, config):
            from typing import Protocol

            class Clock(Protocol):
                async def now(self) -> int: ...

            released["handle"] = context.services.require("clock", Clock)
            raise RuntimeError("second setup failed")

        sys.modules["m3_keep_b"].Plugin.setup = broken_setup
        with pytest.raises(PluginActivationError):
            run(host.activate())
        assert host.get_state("clock") is FiberState.ACTIVE
        assert host.get_state("consumer-a") is FiberState.ACTIVE
        assert host.get_state("consumer-b") is FiberState.FAILED
        assert run(kept.service.now()) == 9
        with pytest.raises(m3_api.ServiceUnavailableError):
            run(released["handle"].service.now())
    finally:
        unload("m3_keep_clock", "m3_keep_a", "m3_keep_b")


def test_stop_waits_for_inflight_call_without_aborting_it(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(
        tmp_path,
        "m3_drain_clock",
        header(event_path)
        + "import m3_drain_gate\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        context.services.provide('clock', self)\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        emit('clock:stop')\n"
        + "    async def now(self):\n"
        + "        m3_drain_gate.entered.set()\n"
        + "        await m3_drain_gate.release.wait()\n"
        + "        return 7\n",
    )
    write_module(
        tmp_path,
        "m3_drain_consumer",
        header(event_path)
        + clock_protocol()
        + "HANDLE = None\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global HANDLE\n"
        + "        HANDLE = context.services.require('clock', Clock)\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        emit('consumer:stop')\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        "m3_drain_clock",
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    consumer = definition(
        tmp_path,
        "consumer",
        "m3_drain_consumer",
        services=(ServiceDependency("clock", ">=1", False),),
    )
    host = make_host((clock, consumer), write_config(tmp_path, ("clock", "consumer")))

    async def main():
        from types import ModuleType

        gate = ModuleType("m3_drain_gate")
        gate.entered = asyncio.Event()
        gate.release = asyncio.Event()
        sys.modules["m3_drain_gate"] = gate
        await host.activate()
        handle = sys.modules["m3_drain_consumer"].HANDLE
        call = asyncio.create_task(handle.service.now())
        await asyncio.wait_for(gate.entered.wait(), timeout=2)
        stop_task = asyncio.create_task(host.stop())
        for _ in range(50):
            await asyncio.sleep(0)
            recorded = events(event_path)
            if "consumer:stop" in recorded and "clock:stop" not in recorded:
                break
        else:
            raise AssertionError(events(event_path))
        with pytest.raises(m3_api.ServiceUnavailableError) as caught:
            await asyncio.wait_for(handle.service.now(), timeout=1)
        assert caught.value.name == "clock"
        assert caught.value.generation == handle.generation
        gate.release.set()
        assert await call == 7
        await stop_task
        assert "clock:stop" in events(event_path)
        assert host.get_state("clock") is FiberState.PENDING

    try:
        run(main())
    finally:
        unload("m3_drain_clock", "m3_drain_consumer", "m3_drain_gate")


def test_new_generation_does_not_revive_old_handle(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3_generation_clock",
        "CALLS = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        context.services.provide('clock', self)\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n"
        "    async def now(self):\n"
        "        CALLS.append(id(self))\n"
        "        return id(self)\n",
    )
    write_module(
        tmp_path,
        "m3_generation_consumer",
        clock_protocol()
        + "HANDLE = None\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global HANDLE\n"
        + "        HANDLE = context.services.require('clock', Clock)\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        return None\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        "m3_generation_clock",
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    consumer = definition(
        tmp_path,
        "consumer",
        "m3_generation_consumer",
        services=(ServiceDependency("clock", ">=1", False),),
    )
    host = make_host((clock, consumer), write_config(tmp_path, ("clock", "consumer")))
    try:
        run(host.activate())
        old = sys.modules["m3_generation_consumer"].HANDLE
        old_generation = old.generation
        old_state = host._services._current["clock"]
        run(host.stop())
        assert old_state.revoked is True
        assert "clock" not in host._services._current
        assert "consumer" not in host._services._current
        assert not any(key[0] in {"clock", "consumer"} for key in host._services._states)
        run(host.activate())
        assert sum(key[0] == "clock" for key in host._services._states) == 1
        assert sum(key[0] == "consumer" for key in host._services._states) == 1
        new = sys.modules["m3_generation_consumer"].HANDLE
        assert new.generation > old_generation
        with pytest.raises(m3_api.ServiceUnavailableError):
            run(old.service.now())
        assert sys.modules["m3_generation_clock"].CALLS == []
        assert run(new.service.now()) == sys.modules["m3_generation_clock"].CALLS[0]
    finally:
        unload("m3_generation_clock", "m3_generation_consumer")


def test_cancellation_revokes_services_activated_in_this_call(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(
        tmp_path,
        "m3_cancel_clock",
        header(event_path)
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        context.services.provide('clock', self)\n"
        + "    async def start(self):\n"
        + "        emit('clock:start')\n"
        + "    async def stop(self):\n"
        + "        emit('clock:stop')\n"
        + "    async def now(self):\n"
        + "        return 1\n",
    )
    write_module(
        tmp_path,
        "m3_cancel_consumer",
        header(event_path)
        + clock_protocol()
        + "import asyncio\n"
        + "import m3_cancel_gate\n"
        + "HANDLE = None\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global HANDLE\n"
        + "        HANDLE = context.services.require('clock', Clock)\n"
        + "        m3_cancel_gate.ready.set()\n"
        + "        await asyncio.sleep(30)\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        emit('consumer:stop')\n",
    )
    clock = definition(
        tmp_path,
        "clock",
        "m3_cancel_clock",
        provides=(ProvidedService("clock", "1.4.0"),),
    )
    consumer = definition(
        tmp_path,
        "consumer",
        "m3_cancel_consumer",
        services=(ServiceDependency("clock", ">=1", False),),
    )
    host = make_host((clock, consumer), write_config(tmp_path, ("clock", "consumer")))

    async def main():
        from types import ModuleType

        gate = ModuleType("m3_cancel_gate")
        gate.ready = asyncio.Event()
        sys.modules["m3_cancel_gate"] = gate
        task = asyncio.create_task(host.activate())
        ready_task = asyncio.create_task(gate.ready.wait())
        done, pending = await asyncio.wait(
            {task, ready_task},
            timeout=2,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            ready_task.cancel()
            await task
            raise AssertionError("activation finished before cancellation")
        for item in pending:
            item.cancel()
        if ready_task not in done:
            task.cancel()
            raise AssertionError("consumer setup did not become ready to cancel")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    try:
        run(main())
        handle = sys.modules["m3_cancel_consumer"].HANDLE
        assert "consumer:stop" not in events(event_path)
        assert "clock:stop" in events(event_path)
        assert host.get_state("clock") is FiberState.PENDING
        assert host.get_state("consumer") is FiberState.FAILED
        with pytest.raises(m3_api.ServiceUnavailableError):
            run(handle.service.now())
    finally:
        unload("m3_cancel_clock", "m3_cancel_consumer", "m3_cancel_gate")


def test_failed_setup_revokes_its_generation(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(
        tmp_path,
        "m3_setup_fail",
        header(event_path)
        + "STATE = None\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global STATE\n"
        + "        STATE = context.services._registry._current[context.instance_id]\n"
        + "        raise RuntimeError('setup failed')\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        emit('clock:stop')\n",
    )
    clock = definition(tmp_path, "clock", "m3_setup_fail")
    host = make_host((clock,), write_config(tmp_path, ("clock",)))
    try:
        with pytest.raises(PluginActivationError):
            run(host.activate())
        state = sys.modules["m3_setup_fail"].STATE
        assert state.revoked is True
        assert state.accepting is False
        assert "clock" not in host._services._current
        assert not any(key[0] == "clock" for key in host._services._states)
        assert "clock:stop" not in events(event_path)
        assert host.get_state("clock") is FiberState.FAILED
    finally:
        unload("m3_setup_fail")


def test_cancelled_setup_revokes_its_generation(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(
        tmp_path,
        "m3_setup_cancel",
        header(event_path)
        + "import asyncio\n"
        + "import m3_setup_cancel_gate\n"
        + "STATE = None\n"
        + "class Plugin:\n"
        + "    async def setup(self, context, config):\n"
        + "        global STATE\n"
        + "        STATE = context.services._registry._current[context.instance_id]\n"
        + "        m3_setup_cancel_gate.ready.set()\n"
        + "        await asyncio.sleep(30)\n"
        + "    async def start(self):\n"
        + "        return None\n"
        + "    async def stop(self):\n"
        + "        emit('clock:stop')\n",
    )
    clock = definition(tmp_path, "clock", "m3_setup_cancel")
    host = make_host((clock,), write_config(tmp_path, ("clock",)))

    async def main():
        from types import ModuleType

        gate = ModuleType("m3_setup_cancel_gate")
        gate.ready = asyncio.Event()
        sys.modules["m3_setup_cancel_gate"] = gate
        task = asyncio.create_task(host.activate())
        ready_task = asyncio.create_task(gate.ready.wait())
        done, pending = await asyncio.wait(
            {task, ready_task},
            timeout=2,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            ready_task.cancel()
            await task
            raise AssertionError("activation finished before cancellation")
        for item in pending:
            item.cancel()
        if ready_task not in done:
            task.cancel()
            raise AssertionError("setup did not become ready to cancel")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    try:
        run(main())
        state = sys.modules["m3_setup_cancel"].STATE
        assert state.revoked is True
        assert state.accepting is False
        assert "clock" not in host._services._current
        assert not any(key[0] == "clock" for key in host._services._states)
        assert "clock:stop" not in events(event_path)
        assert host.get_state("clock") is FiberState.FAILED
    finally:
        unload("m3_setup_cancel", "m3_setup_cancel_gate")


def test_construct_failure_revoke_is_a_noop(m3_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(
        tmp_path,
        "m3_construct_fail",
        "class Plugin:\n    def __init__(self):\n        raise RuntimeError('construct failed')\n",
    )
    clock = definition(tmp_path, "clock", "m3_construct_fail")
    host = make_host((clock,), write_config(tmp_path, ("clock",)))
    try:
        with pytest.raises(PluginLoadError):
            run(host.activate())
        assert host._services._current == {}
        assert host._services._states == {}
        assert host.get_state("clock") is FiberState.FAILED
    finally:
        unload("m3_construct_fail")
