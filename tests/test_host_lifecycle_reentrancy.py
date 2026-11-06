"""Reentrant lifecycle requests must fail before changing host state."""

import asyncio
import sys
from types import ModuleType
from typing import Protocol

import pytest

from soul_plugin_core.capabilities import CapabilityPolicy
from soul_plugin_core.contracts import (
    PluginDefinition,
    PluginDependency,
    ProvidedService,
    ServiceDependency,
)
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import PluginActivationError
from soul_plugin_core.host import PluginHost
from soul_plugin_core.lifecycle import FiberState


class Service(Protocol):
    async def invoke(self) -> object: ...


@pytest.fixture
def runtime(monkeypatch, tmp_path):
    module = ModuleType("lifecycle_reentrancy_fixture")
    callback = None
    handles = {}

    class Plugin:
        async def setup(self, context, config):
            self.id = context.instance_id
            if self.id in {"p", "q"}:
                context.services.provide(self.id, self)
            if self.id == "client":
                for name in ("p", "q"):
                    handles[name] = context.services.require(name, Service)

        async def start(self):
            pass

        async def stop(self):
            pass

        async def invoke(self):
            return await callback(self.id)

    module.Plugin = Plugin
    monkeypatch.setitem(sys.modules, module.__name__, module)

    def definition(name, *, deps=(), services=(), provides=()):
        return PluginDefinition(
            plugin_id=name,
            version="1",
            api_version="1",
            entry_point=f"{module.__name__}:Plugin",
            plugin_dependencies=tuple(PluginDependency(dep, "", False) for dep in deps),
            service_dependencies=tuple(ServiceDependency(dep, "", False) for dep in services),
            provided_services=tuple(ProvidedService(name, "1") for name in provides),
            capability_requests=(),
            config_schema=None,
            source=f"{name}/plugin.toml",
        )

    definitions = (
        definition("p", provides=("p",)),
        definition("q", deps=("p",), provides=("q",)),
        definition("client", services=("p", "q")),
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    host = PluginHost(definitions, build_activation_plan(definitions), config_path=config)

    def set_callback(value):
        nonlocal callback
        callback = value

    return host, handles, set_callback


@pytest.mark.parametrize("operation", ["deactivate", "stop", "dispose", "cascade"])
def test_service_cannot_wait_for_its_own_shutdown(runtime, operation):
    host, handles, set_callback = runtime

    async def request(instance_id):
        if operation == "deactivate":
            await host.deactivate(instance_id)
        elif operation == "cascade":
            await host.deactivate("p")
        else:
            await getattr(host, operation)()

    async def scenario():
        set_callback(request)
        await host.activate()
        handle = handles["q" if operation == "cascade" else "p"]
        with pytest.raises(PluginActivationError, match="active service call"):
            await asyncio.wait_for(handle.service.invoke(), 1)
        assert all(host.get_state(name) is FiberState.ACTIVE for name in ("p", "q", "client"))
        assert not host.is_disabled("p")
        await host.dispose()

    asyncio.run(scenario())


def test_child_task_inherits_live_call_but_not_stale_frame(runtime):
    host, handles, set_callback = runtime
    children = []

    async def request(instance_id):
        async def immediate():
            await host.deactivate(instance_id)

        with pytest.raises(PluginActivationError, match="active service call"):
            await asyncio.create_task(immediate())
        release = asyncio.Event()

        async def after_return():
            await release.wait()
            await host.deactivate(instance_id)

        children.append((asyncio.create_task(after_return()), release))

    async def scenario():
        set_callback(request)
        await host.activate()
        await handles["p"].service.invoke()
        child, release = children[0]
        release.set()
        await asyncio.wait_for(child, 1)
        assert host.is_disabled("p")
        await host.dispose()

    asyncio.run(scenario())


def test_unrelated_shutdown_is_allowed_during_service_call(runtime):
    host, handles, set_callback = runtime

    async def request(instance_id):
        assert instance_id == "p"
        await host.deactivate("q")

    async def scenario():
        set_callback(request)
        await host.activate()
        await asyncio.wait_for(handles["p"].service.invoke(), 1)
        assert host.is_disabled("q")
        assert host.get_state("p") is FiberState.ACTIVE
        await host.dispose()

    asyncio.run(scenario())


def test_nested_service_calls_check_entire_stack(runtime):
    host, handles, set_callback = runtime

    async def request(instance_id):
        if instance_id == "q":
            return await handles["p"].service.invoke()
        await host.deactivate("q")

    async def scenario():
        set_callback(request)
        await host.activate()
        with pytest.raises(PluginActivationError, match="active service call"):
            await asyncio.wait_for(handles["q"].service.invoke(), 1)
        assert host.get_state("q") is FiberState.ACTIVE
        await host.dispose()

    asyncio.run(scenario())


def test_matching_instance_id_in_another_host_does_not_block_shutdown(runtime):
    host, handles, set_callback = runtime
    other = PluginHost(tuple(host._definitions.values()), host._plan, config_path=host._config_path)

    async def request(instance_id):
        await other.deactivate(instance_id)

    async def scenario():
        set_callback(request)
        await host.activate()
        first_handle = handles["p"]
        await other.activate()
        await asyncio.wait_for(first_handle.service.invoke(), 1)
        assert host.get_state("p") is FiberState.ACTIVE
        assert other.is_disabled("p")
        await host.dispose()
        await other.dispose()

    asyncio.run(scenario())


def test_service_does_not_wait_for_a_concurrent_draining_host_lock(runtime):
    host, handles, set_callback = runtime

    async def scenario():
        started = asyncio.Event()
        resume = asyncio.Event()

        async def request(instance_id):
            started.set()
            await resume.wait()
            with pytest.raises(PluginActivationError, match="host draining"):
                await host.activate()

        set_callback(request)
        await host.activate()
        call = asyncio.create_task(handles["p"].service.invoke())
        await started.wait()
        stopping = asyncio.create_task(host.stop())
        while host.get_state("p") is not FiberState.UNLOADING:
            await asyncio.sleep(0)
        resume.set()
        await asyncio.wait_for(call, 1)
        await asyncio.wait_for(stopping, 1)
        await host.dispose()

    asyncio.run(scenario())


def test_service_called_during_activation_rejects_reentry_before_lock(runtime, monkeypatch):
    host, handles, set_callback = runtime
    plugin_type = sys.modules["lifecycle_reentrancy_fixture"].Plugin

    async def request(instance_id):
        await host.deactivate(instance_id)

    async def start(plugin):
        if plugin.id == "client":
            with pytest.raises(PluginActivationError, match="active service call"):
                await handles["p"].service.invoke()

    async def scenario():
        set_callback(request)
        monkeypatch.setattr(plugin_type, "start", start)
        await asyncio.wait_for(host.activate(), 1)
        assert host.get_state("p") is FiberState.ACTIVE
        await host.dispose()

    asyncio.run(scenario())


def test_lifecycle_hook_reentry_fails_without_locking(monkeypatch, tmp_path):
    module = ModuleType("lifecycle_hook_reentrancy")
    holder = {}

    class Plugin:
        async def setup(self, context, config):
            pass

        async def start(self):
            with pytest.raises(PluginActivationError, match="lifecycle hooks"):
                await holder["host"].stop()

        async def stop(self):
            pass

    module.Plugin = Plugin
    monkeypatch.setitem(sys.modules, module.__name__, module)
    definition = PluginDefinition(
        plugin_id="p",
        version="1",
        api_version="1",
        entry_point=f"{module.__name__}:Plugin",
        plugin_dependencies=(),
        service_dependencies=(),
        provided_services=(),
        capability_requests=(),
        config_schema=None,
        source="p/plugin.toml",
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    host = PluginHost((definition,), build_activation_plan((definition,)), config_path=config)
    holder["host"] = host

    async def scenario():
        await asyncio.wait_for(host.activate(), 1)
        assert host.get_state("p") is FiberState.ACTIVE
        await host.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["events", "extensions"])
@pytest.mark.parametrize("target", ["publisher", "observer", "stop", "dispose"])
def test_observer_cannot_stop_inflight_publisher_or_self(monkeypatch, tmp_path, channel, target):
    module = ModuleType("event_reentrancy_fixture")
    contexts = {}
    holder = {}
    children = []

    class Plugin:
        async def setup(self, context, config):
            self.id = context.instance_id
            contexts[self.id] = context
            if self.id == "observer":

                async def observe(value):
                    if target in {"stop", "dispose"}:
                        operation = getattr(holder["host"], target)()
                    else:
                        operation = holder["host"].deactivate(target)
                    with pytest.raises(PluginActivationError, match="event callback"):
                        await operation
                    if target == "observer":
                        release = asyncio.Event()

                        async def after_callback():
                            await release.wait()
                            await holder["host"].deactivate("observer")

                        children.append((asyncio.create_task(after_callback()), release))

                if channel == "events":
                    context.events.subscribe("topic", observe)
                else:
                    context.extensions.register("topic", observe)

        async def start(self):
            pass

        async def stop(self):
            pass

    module.Plugin = Plugin
    monkeypatch.setitem(sys.modules, module.__name__, module)
    grants = {
        "publisher": ("events.publish", "extensions.invoke"),
        "observer": ("events.subscribe", "extensions.register"),
    }
    definitions = tuple(
        PluginDefinition(
            plugin_id=name,
            version="1",
            api_version="1",
            entry_point=f"{module.__name__}:Plugin",
            plugin_dependencies=(),
            service_dependencies=(),
            provided_services=(),
            capability_requests=grants[name],
            config_schema=None,
            source=f"{name}/plugin.toml",
        )
        for name in grants
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    host = PluginHost(
        definitions,
        build_activation_plan(definitions),
        config_path=config,
        capability_policy=CapabilityPolicy(grants),
    )
    holder["host"] = host

    async def scenario():
        await host.activate()
        if channel == "events":
            await asyncio.wait_for(contexts["publisher"].events.publish("topic", None), 1)
        else:
            await asyncio.wait_for(contexts["publisher"].extensions.invoke("topic", None), 1)
        assert all(host.get_state(name) is FiberState.ACTIVE for name in grants)
        if children:
            child, release = children[0]
            release.set()
            await asyncio.wait_for(child, 1)
            assert host.is_disabled("observer")
        await host.dispose()

    asyncio.run(scenario())
