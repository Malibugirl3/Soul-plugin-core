"""固定实例绑定上的显式停用、强依赖级联和恢复。"""

import asyncio
import sys
from types import ModuleType
from typing import Protocol

import pytest

from soul_plugin_core.contracts import (
    PluginDefinition,
    PluginDependency,
    ProvidedService,
    ServiceDependency,
)
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import (
    PluginActivationError,
    PluginCleanupError,
    ServiceUnavailableError,
)
from soul_plugin_core.host import PluginHost
from soul_plugin_core.instances import InstanceBindingSelection, InstanceDefinition
from soul_plugin_core.lifecycle import FiberState


class Clock(Protocol):
    async def now(self) -> int: ...


def definition(name, *, plugins=(), services=(), provides=()):
    return PluginDefinition(
        plugin_id=name,
        version="1.0",
        api_version="1",
        entry_point="deactivation_fixture:Plugin",
        plugin_dependencies=tuple(PluginDependency(p, "", optional) for p, optional in plugins),
        service_dependencies=tuple(ServiceDependency(s, "", optional) for s, optional in services),
        provided_services=tuple(ProvidedService(s, "1") for s in provides),
        capability_requests=(),
        config_schema=None,
        source=f"{name}/plugin.toml",
    )


@pytest.fixture
def plugin_runtime(monkeypatch):
    module = ModuleType("deactivation_fixture")
    events = []
    handles = {}
    failures = set()
    waiters = {}
    defs = {}

    class Plugin:
        async def setup(self, context, config):
            self.instance_id = context.instance_id
            events.append(("setup", self.instance_id))
            for provided in defs[context.plugin_id].provided_services:
                context.services.provide(provided.name, self)
            for dep in defs[context.plugin_id].service_dependencies:
                if dep.optional:
                    handle = context.services.optional(dep.name, Clock)
                else:
                    handle = context.services.require(dep.name, Clock)
                handles[self.instance_id, dep.name] = handle

        async def start(self):
            events.append(("start", self.instance_id))
            if f"start:{self.instance_id}" in failures:
                raise RuntimeError(f"start:{self.instance_id}")

        async def stop(self):
            events.append(("stop", self.instance_id))
            if self.instance_id in waiters:
                started, release = waiters[self.instance_id]
                started.set()
                await release.wait()
            if self.instance_id in failures:
                raise RuntimeError(self.instance_id)

        async def now(self):
            return 42

    module.Plugin = Plugin
    monkeypatch.setitem(sys.modules, module.__name__, module)

    def create(tmp_path, declarations, instances=None):
        defs.update({item.plugin_id: item for item in declarations})
        config = tmp_path / "config.toml"
        config.write_text("", encoding="utf-8")
        return PluginHost(
            declarations,
            build_activation_plan(declarations),
            config_path=config,
            instances=instances,
        )

    return create, events, handles, failures, waiters


def test_transitive_cascade_shared_provider_and_explicit_recovery(plugin_runtime, tmp_path):
    create, events, _, _, _ = plugin_runtime
    declarations = (
        definition("a"),
        definition("b", plugins=(("a", False),)),
        definition("c", plugins=(("b", False),)),
        definition("d"),
        definition("x", plugins=(("a", False),)),
    )
    host = create(tmp_path, declarations)

    async def scenario():
        await host.activate()
        events.clear()
        await host.deactivate("b")
        assert events == [("stop", "c"), ("stop", "b")]
        assert all(host.get_state(x) is FiberState.ACTIVE for x in ("a", "d", "x"))
        assert host.get_blockers("c") == ("b",)
        assert host.is_disabled("b")
        events.clear()
        await host.activate()
        assert events == []
        await host.deactivate("b")
        assert events == []
        await host.activate_instance("b")
        assert not host.is_disabled("b")
        assert host.get_state("c") is FiberState.PENDING
        await host.activate()
        assert host.get_state("c") is FiberState.ACTIVE
        await host.dispose()

    asyncio.run(scenario())


def test_multiple_actual_bindings_and_mixed_strong_optional_edges(plugin_runtime, tmp_path):
    create, events, _, _, _ = plugin_runtime
    declarations = (
        definition("provider", provides=("clock",)),
        definition("worker", plugins=(("provider", False),), services=(("clock", True),)),
    )
    instances = (
        InstanceDefinition("p1", "provider"),
        InstanceDefinition("p2", "provider"),
        InstanceDefinition(
            "w1",
            "worker",
            (
                InstanceBindingSelection("plugin", "provider", "p1"),
                InstanceBindingSelection("service", "clock", "p1"),
            ),
        ),
        InstanceDefinition(
            "w2",
            "worker",
            (
                InstanceBindingSelection("plugin", "provider", "p2"),
                InstanceBindingSelection("service", "clock", "p2"),
            ),
        ),
    )
    host = create(tmp_path, declarations, instances)

    async def scenario():
        await host.activate()
        events.clear()
        await host.deactivate("p1")
        assert events == [("stop", "w1"), ("stop", "p1")]
        assert host.get_state("w2") is FiberState.ACTIVE
        assert host.get_state("p2") is FiberState.ACTIVE
        with pytest.raises(PluginActivationError):
            await host.activate_instance("w1")
        await host.activate_instance("p1")
        await host.activate()
        assert host.get_state("w1") is FiberState.ACTIVE
        await host.dispose()

    asyncio.run(scenario())


def test_optional_generation_and_unbound_optional(plugin_runtime, tmp_path):
    create, _, handles, _, _ = plugin_runtime
    host = create(
        tmp_path,
        (
            definition("provider", provides=("clock",)),
            definition("optional", services=(("clock", True),)),
            definition("missing", services=(("not-provided", True),)),
        ),
    )

    async def scenario():
        await host.activate()
        old = handles["optional", "clock"]
        assert handles["missing", "not-provided"] is None
        await host.deactivate("provider")
        assert host.get_state("optional") is FiberState.ACTIVE
        assert host.get_blockers("optional") == ()
        assert host.get_binding_blockers("optional") == ("provider",)
        with pytest.raises(ServiceUnavailableError):
            await old.service.now()
        await host.deactivate("optional")
        with pytest.raises(PluginActivationError):
            await host.activate_instance("optional")
        await host.activate_instance("provider")
        with pytest.raises(ServiceUnavailableError):
            await old.service.now()
        await host.activate_instance("optional")
        new = handles["optional", "clock"]
        assert new.generation > old.generation
        assert await new.service.now() == 42
        await host.dispose()

    asyncio.run(scenario())


def test_failed_cleanup_continues_cascade_and_preserves_disabled(plugin_runtime, tmp_path):
    create, events, _, failures, _ = plugin_runtime
    host = create(
        tmp_path,
        (
            definition("a"),
            definition("b", plugins=(("a", False),)),
            definition("c", plugins=(("b", False),)),
        ),
    )

    async def scenario():
        await host.activate()
        failures.add("b")
        events.clear()
        with pytest.raises(PluginCleanupError) as caught:
            await host.deactivate("a")
        assert [str(e) for e in caught.value.errors] == ["b"]
        assert events == [("stop", "c"), ("stop", "b"), ("stop", "a")]
        assert host.get_state("b") is FiberState.FAILED
        assert host.is_disabled("a")
        await host.activate_instance("a")
        with pytest.raises(PluginActivationError):
            await host.activate()
        assert host.get_state("b") is FiberState.FAILED
        await host.dispose()

    asyncio.run(scenario())


def test_repeated_cancellation_does_not_interrupt_cascade(plugin_runtime, tmp_path):
    create, events, _, _, waiters = plugin_runtime
    host = create(
        tmp_path,
        (
            definition("a"),
            definition("b", plugins=(("a", False),)),
            definition("c", plugins=(("b", False),)),
        ),
    )

    async def scenario():
        await host.activate()
        events.clear()
        started, release = asyncio.Event(), asyncio.Event()
        waiters["c"] = started, release
        task = asyncio.create_task(host.deactivate("a"))
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert events == [("stop", "c"), ("stop", "b"), ("stop", "a")]
        assert host.is_disabled("a")
        assert all(host.get_state(x) is FiberState.PENDING for x in ("a", "b", "c"))
        await host.dispose()

    asyncio.run(scenario())


def test_multiple_blockers_and_failed_restore_do_not_reactivate_consumer(plugin_runtime, tmp_path):
    create, _, _, failures, _ = plugin_runtime
    host = create(
        tmp_path,
        (
            definition("a"),
            definition("b"),
            definition("c", plugins=(("a", False), ("b", False))),
        ),
    )

    async def scenario():
        await host.activate()
        await host.deactivate("a")
        await host.deactivate("b")
        assert host.get_blockers("c") == ("a", "b")
        await host.activate_instance("a")
        await host.activate()
        assert host.get_blockers("c") == ("b",)
        assert host.get_state("c") is FiberState.PENDING
        failures.add("start:b")
        with pytest.raises(PluginActivationError):
            await host.activate_instance("b")
        assert host.is_disabled("b")
        assert host.get_state("b") is FiberState.FAILED
        assert host.get_state("c") is FiberState.PENDING
        with pytest.raises(PluginActivationError):
            await host.activate_instance("b")
        await host.dispose()

    asyncio.run(scenario())
