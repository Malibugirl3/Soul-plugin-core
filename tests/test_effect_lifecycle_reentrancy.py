"""Effect callbacks must not wait on a lifecycle that will join them."""

import asyncio
import sys
from types import ModuleType

import pytest

from soul_plugin_core.context import ActivationScope
from soul_plugin_core.contracts import PluginDefinition, PluginDependency
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import PluginActivationError
from soul_plugin_core.host import PluginHost
from soul_plugin_core.lifecycle import FiberState


@pytest.mark.parametrize("operation", ["deactivate", "cascade", "stop", "dispose"])
@pytest.mark.parametrize("effect", ["task", "cleanup", "async_context"])
def test_owned_effect_cannot_await_host_shutdown(monkeypatch, tmp_path, operation, effect):
    module = ModuleType("effect_host_reentrancy")
    holder = {}
    trigger = asyncio.Event()
    invoked = asyncio.Event()

    async def request():
        await trigger.wait()
        if operation in {"deactivate", "cascade"}:
            call = holder["host"].deactivate("owner" if operation == "deactivate" else "parent")
        else:
            call = getattr(holder["host"], operation)()
        with pytest.raises(PluginActivationError, match="effect"):
            await call
        invoked.set()

    class Manager:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            await request()

    class Plugin:
        async def setup(self, context, config):
            if context.instance_id != "owner":
                return
            if effect == "task":
                holder["handle"] = context.effects.create_task(request())
            elif effect == "cleanup":
                holder["handle"] = context.effects.defer(request)
            else:
                child = context.effects.child_scope()
                await child.enter_async_context(Manager())
                holder["handle"] = child

        async def start(self):
            pass

        async def stop(self):
            pass

    module.Plugin = Plugin
    monkeypatch.setitem(sys.modules, module.__name__, module)

    def definition(name):
        return PluginDefinition(
            plugin_id=name,
            version="1",
            api_version="1",
            entry_point=f"{module.__name__}:Plugin",
            plugin_dependencies=(PluginDependency("parent", "", False),) if name == "owner" else (),
            service_dependencies=(),
            provided_services=(),
            capability_requests=(),
            config_schema=None,
            source=f"{name}/plugin.toml",
        )

    definitions = (definition("parent"), definition("owner"))
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    host = PluginHost(definitions, build_activation_plan(definitions), config_path=config)
    holder["host"] = host

    async def scenario():
        await host.activate()
        if effect != "task":
            closing = asyncio.create_task(holder["handle"].aclose())
        trigger.set()
        await asyncio.wait_for(invoked.wait(), 1)
        if effect == "task":
            await asyncio.wait_for(holder["handle"].task, 1)
        else:
            await asyncio.wait_for(closing, 1)
        assert all(host.get_state(name) is FiberState.ACTIVE for name in ("parent", "owner"))
        assert not host.is_disabled("owner")
        await host.dispose()

    asyncio.run(scenario())


def test_scope_and_task_handles_reject_self_join():
    async def scenario():
        scope = ActivationScope("plugin", "instance")
        child = scope.child_scope()
        proceed = asyncio.Event()
        results = []

        async def work():
            await proceed.wait()
            with pytest.raises(PluginActivationError, match="owning scope"):
                await scope.aclose()
            with pytest.raises(PluginActivationError, match="owning scope"):
                await child.aclose()
            with pytest.raises(PluginActivationError, match="own handle"):
                await handle.aclose()
            results.append("done")

        handle = child.create_task(work())
        proceed.set()
        await asyncio.wait_for(handle.task, 1)
        await asyncio.wait_for(scope.aclose(), 1)
        assert results == ["done"]

    asyncio.run(scenario())


def test_cleanup_handle_rejects_self_join():
    async def scenario():
        scope = ActivationScope("plugin", "instance")

        async def cleanup():
            with pytest.raises(PluginActivationError, match="own handle"):
                await handle.aclose()
            with pytest.raises(PluginActivationError, match="owning scope"):
                await scope.aclose()

        handle = scope.defer(cleanup)
        await asyncio.wait_for(handle.aclose(), 1)
        await scope.aclose()

    asyncio.run(scenario())


def test_inherited_task_marker_expires_after_managed_task_finishes():
    async def scenario():
        scope = ActivationScope("plugin", "instance")
        children = []
        release = asyncio.Event()

        async def work():
            async def child():
                await release.wait()
                await scope.aclose()

            children.append(asyncio.create_task(child()))

        handle = scope.create_task(work())
        await handle.task
        release.set()
        await asyncio.wait_for(children[0], 1)

    asyncio.run(scenario())


def test_inherited_cleanup_marker_expires_after_cleanup_finishes():
    async def scenario():
        scope = ActivationScope("plugin", "instance")
        children = []
        release = asyncio.Event()

        async def cleanup():
            async def child():
                await release.wait()
                await scope.aclose()

            children.append(asyncio.create_task(child()))

        await scope.defer(cleanup).aclose()
        release.set()
        await asyncio.wait_for(children[0], 1)

    asyncio.run(scenario())


def test_managed_task_cannot_wait_for_lock_while_host_stops(monkeypatch, tmp_path):
    module = ModuleType("effect_host_stop_lock")
    holder = {}
    started = asyncio.Event()
    checked = asyncio.Event()

    class Plugin:
        async def setup(self, context, config):
            async def work():
                started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    with pytest.raises(PluginActivationError, match="host draining"):
                        await holder["host"].activate()
                    checked.set()

            context.effects.create_task(work())

        async def start(self):
            pass

        async def stop(self):
            pass

    module.Plugin = Plugin
    monkeypatch.setitem(sys.modules, module.__name__, module)
    definition = PluginDefinition(
        plugin_id="worker",
        version="1",
        api_version="1",
        entry_point=f"{module.__name__}:Plugin",
        plugin_dependencies=(),
        service_dependencies=(),
        provided_services=(),
        capability_requests=(),
        config_schema=None,
        source="worker/plugin.toml",
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    host = PluginHost((definition,), build_activation_plan((definition,)), config_path=config)
    holder["host"] = host

    async def scenario():
        await host.activate()
        await started.wait()
        await asyncio.wait_for(host.stop(), 1)
        assert checked.is_set()
        await host.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("handle", ["child_scope", "effect_handle"])
@pytest.mark.parametrize("preexisting", [False, True])
def test_sync_cleanup_returned_task_cannot_stop_owner(monkeypatch, tmp_path, handle, preexisting):
    module = ModuleType("cleanup_returned_task")
    holder = {}
    release = asyncio.Event()
    executed = asyncio.Event()

    async def worker():
        await release.wait()
        with pytest.raises(PluginActivationError, match="effect"):
            await holder["host"].deactivate("owner")
        executed.set()

    class Plugin:
        async def setup(self, context, config):
            child = context.effects.child_scope()
            holder["child"] = child
            holder["effect"] = child.defer(
                lambda: holder["task"] if preexisting else asyncio.create_task(worker())
            )

        async def start(self):
            pass

        async def stop(self):
            pass

    module.Plugin = Plugin
    monkeypatch.setitem(sys.modules, module.__name__, module)
    definition = PluginDefinition(
        plugin_id="owner",
        version="1",
        api_version="1",
        entry_point=f"{module.__name__}:Plugin",
        plugin_dependencies=(),
        service_dependencies=(),
        provided_services=(),
        capability_requests=(),
        config_schema=None,
        source="owner/plugin.toml",
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    host = PluginHost((definition,), build_activation_plan((definition,)), config_path=config)
    holder["host"] = host

    async def scenario():
        await host.activate()
        if preexisting:
            holder["task"] = asyncio.create_task(worker())
        closer = asyncio.create_task(
            holder["child" if handle == "child_scope" else "effect"].aclose()
        )
        await asyncio.sleep(0)
        release.set()
        await asyncio.wait_for(executed.wait(), 1)
        await asyncio.wait_for(closer, 1)
        assert host.get_state("owner") is FiberState.ACTIVE
        await host.dispose()

    asyncio.run(scenario())
