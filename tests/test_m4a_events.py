"""M4a event delivery, authorization, and activation ownership."""

import asyncio
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest

from soul_plugin_core.capabilities import CapabilityPolicy
from soul_plugin_core.contracts import PluginDefinition, ProvidedService
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import CapabilityDeniedError, EventDispatchError, PluginActivationError
from soul_plugin_core.host import PluginHost
from soul_plugin_core.lifecycle import FiberState


@pytest.fixture
def workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.syspath_prepend(str(tmp_path))
    yield tmp_path
    for name in tuple(sys.modules):
        if name.startswith("m4a_test_"):
            sys.modules.pop(name, None)


def definition(
    root: Path,
    plugin_id: str,
    module: str,
    *requests: str,
    provides: tuple[ProvidedService, ...] = (),
) -> PluginDefinition:
    plugin_dir = root / plugin_id
    plugin_dir.mkdir(parents=True, exist_ok=True)
    manifest = plugin_dir / "plugin.toml"
    manifest.write_text("# source\n", encoding="utf-8")
    return PluginDefinition(
        plugin_id=plugin_id,
        version="1.0",
        api_version="1",
        entry_point=f"{module}:Plugin",
        plugin_dependencies=(),
        service_dependencies=(),
        provided_services=provides,
        capability_requests=tuple(requests),
        config_schema=None,
        source=str(manifest),
    )


def plugin(root: Path, module: str, source: str) -> None:
    (root / f"{module}.py").write_text(source, encoding="utf-8")


def host(root: Path, *definitions: PluginDefinition) -> PluginHost:
    config = root / "config.toml"
    config.write_text("", encoding="utf-8")
    return PluginHost(
        definitions,
        build_activation_plan(definitions),
        config_path=config,
        capability_policy=CapabilityPolicy(
            {item.plugin_id: item.capability_requests for item in definitions}
        ),
    )


def run(coroutine) -> None:
    asyncio.run(asyncio.wait_for(coroutine, timeout=3))


def test_serial_delivery_continues_and_aggregates_failures(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_errors",
        "EVENTS = []\n"
        "CONTEXT = None\n"
        "FAIL1 = ValueError('first failure')\n"
        "FAIL2 = RuntimeError('last failure')\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        global CONTEXT\n"
        "        CONTEXT = context\n"
        "        async def first(payload):\n"
        "            EVENTS.append('first')\n"
        "            raise FAIL1\n"
        "        async def middle(payload):\n"
        "            EVENTS.append('middle')\n"
        "        async def last(payload):\n"
        "            EVENTS.append('last')\n"
        "            raise FAIL2\n"
        "        context.events.subscribe('topic', first)\n"
        "        context.events.subscribe('topic', middle)\n"
        "        context.events.subscribe('topic', last)\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    item = definition(workspace, "worker", "m4a_test_errors", "events.publish", "events.subscribe")
    instance = host(workspace, item)

    async def scenario() -> None:
        await instance.activate()
        module = sys.modules["m4a_test_errors"]
        with pytest.raises(EventDispatchError) as caught:
            await module.CONTEXT.events.publish("topic", "secret payload")
        assert module.EVENTS == ["first", "middle", "last"]
        assert [failure.cause for failure in caught.value.failures] == [module.FAIL1, module.FAIL2]
        assert [failure.instance_id for failure in caught.value.failures] == ["worker", "worker"]
        assert caught.value.event == "topic"
        assert "secret payload" not in str(caught.value)
        await instance.stop()

    run(scenario())


def test_subscriptions_remain_staged_until_start_succeeds(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_publisher",
        "CONTEXT = None\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        global CONTEXT\n"
        "        CONTEXT = context\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    plugin(
        workspace,
        "m4a_test_staged",
        "import m4a_test_gate\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        async def listener(payload):\n"
        "            m4a_test_gate.SEEN.append(payload)\n"
        "        context.events.subscribe('topic', listener)\n"
        "    async def start(self):\n"
        "        m4a_test_gate.ready.set()\n"
        "        await m4a_test_gate.release.wait()\n"
        "    async def stop(self):\n"
        "        return None\n",
    )
    publisher = definition(workspace, "a", "m4a_test_publisher", "events.publish")
    listener = definition(workspace, "b", "m4a_test_staged", "events.subscribe")
    instance = host(workspace, publisher, listener)

    async def scenario() -> None:
        gate = ModuleType("m4a_test_gate")
        gate.ready = asyncio.Event()
        gate.release = asyncio.Event()
        gate.SEEN = []
        sys.modules["m4a_test_gate"] = gate
        activation = asyncio.create_task(instance.activate())
        await gate.ready.wait()
        context = sys.modules["m4a_test_publisher"].CONTEXT
        await context.events.publish("topic", "before")
        assert gate.SEEN == []
        gate.release.set()
        await activation
        await context.events.publish("topic", "after")
        assert gate.SEEN == ["after"]
        await instance.stop()

    run(scenario())


def test_stop_drains_callbacks_before_stop_and_rejects_new_delivery(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_pub",
        "CONTEXT = None\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        global CONTEXT\n"
        "        CONTEXT = context\n"
        "    async def start(self): return None\n"
        "    async def stop(self): return None\n",
    )
    plugin(
        workspace,
        "m4a_test_sub",
        "import m4a_test_gate\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        async def listener(payload):\n"
        "            m4a_test_gate.SEEN.append(payload + ':enter')\n"
        "            if payload == 'slow':\n"
        "                m4a_test_gate.ready.set()\n"
        "                await m4a_test_gate.release.wait()\n"
        "            m4a_test_gate.SEEN.append(payload + ':exit')\n"
        "        context.events.subscribe('topic', listener)\n"
        "    async def start(self): return None\n"
        "    async def stop(self):\n"
        "        m4a_test_gate.SEEN.append('stop')\n",
    )
    publisher = definition(workspace, "a", "m4a_test_pub", "events.publish")
    listener = definition(workspace, "b", "m4a_test_sub", "events.subscribe")
    instance = host(workspace, publisher, listener)

    async def scenario() -> None:
        gate = ModuleType("m4a_test_gate")
        gate.ready = asyncio.Event()
        gate.release = asyncio.Event()
        gate.SEEN = []
        sys.modules["m4a_test_gate"] = gate
        await instance.activate()
        context = sys.modules["m4a_test_pub"].CONTEXT
        pending = asyncio.create_task(context.events.publish("topic", "slow"))
        await gate.ready.wait()
        stopping = asyncio.create_task(instance.stop())
        while instance.get_state("b") is not FiberState.UNLOADING:
            await asyncio.sleep(0)
        await context.events.publish("topic", "new")
        assert gate.SEEN == ["slow:enter"]
        assert not stopping.done()
        gate.release.set()
        await pending
        await stopping
        assert gate.SEEN == ["slow:enter", "slow:exit", "stop"]

    run(scenario())


def test_cancelled_stop_waits_for_event_drain_before_propagating(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_cancel_stop",
        "import m4a_test_gate\n"
        "CONTEXT = None\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        global CONTEXT\n"
        "        CONTEXT = context\n"
        "        async def listener(payload):\n"
        "            m4a_test_gate.ready.set()\n"
        "            await m4a_test_gate.release.wait()\n"
        "            m4a_test_gate.SEEN.append('listener-exit')\n"
        "        context.events.subscribe('topic', listener)\n"
        "    async def start(self): return None\n"
        "    async def stop(self): m4a_test_gate.SEEN.append('stop')\n",
    )
    item = definition(
        workspace, "worker", "m4a_test_cancel_stop", "events.publish", "events.subscribe"
    )
    instance = host(workspace, item)

    async def scenario() -> None:
        gate = ModuleType("m4a_test_gate")
        gate.ready = asyncio.Event()
        gate.release = asyncio.Event()
        gate.SEEN = []
        gate.CANCELLED = []
        gate.PROPAGATED = []
        sys.modules["m4a_test_gate"] = gate
        await instance.activate()
        context = sys.modules["m4a_test_cancel_stop"].CONTEXT
        pending = asyncio.create_task(context.events.publish("topic", None))
        await gate.ready.wait()
        original_drain = instance._events.drain

        async def observed_drain(instance_id: str) -> None:
            try:
                await original_drain(instance_id)
            except asyncio.CancelledError as exc:
                gate.CANCELLED.append(exc)
                raise

        async def observed_stop() -> None:
            try:
                await instance.stop()
            except asyncio.CancelledError as exc:
                gate.PROPAGATED.append(exc)
                raise

        instance._events.drain = observed_drain
        stopping = asyncio.create_task(observed_stop())
        while instance.get_state("worker") is not FiberState.UNLOADING:
            await asyncio.sleep(0)
        stopping.cancel("stop-cancelled")
        await asyncio.sleep(0)
        assert not stopping.done()
        assert gate.SEEN == []
        gate.release.set()
        await pending
        with pytest.raises(asyncio.CancelledError):
            await stopping
        assert len(gate.CANCELLED) == len(gate.PROPAGATED) == 1
        assert gate.PROPAGATED[0] is gate.CANCELLED[0]
        assert gate.PROPAGATED[0].args == ("stop-cancelled",)
        assert gate.SEEN == ["listener-exit", "stop"]
        assert instance.get_state("worker") is FiberState.PENDING
        assert instance._events._topics == {}
        assert instance._events._owners == {}

    run(scenario())


def test_event_capability_is_enforced_for_publish_and_subscribe(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_unauthorized",
        "CONTEXT = None\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        global CONTEXT\n"
        "        CONTEXT = context\n"
        "    async def start(self): return None\n"
        "    async def stop(self): return None\n",
    )
    item = definition(workspace, "worker", "m4a_test_unauthorized")
    instance = host(workspace, item)

    async def scenario() -> None:
        await instance.activate()
        context = sys.modules["m4a_test_unauthorized"].CONTEXT

        async def listener(payload):
            return None

        with pytest.raises(CapabilityDeniedError) as subscribe:
            context.events.subscribe("topic", listener)
        assert subscribe.value.missing == ("events.subscribe",)
        assert subscribe.value.instance_id == "worker"
        with pytest.raises(CapabilityDeniedError) as publish:
            await context.events.publish("topic", "payload")
        assert publish.value.missing == ("events.publish",)
        await instance.stop()

    run(scenario())


def test_event_and_service_ingress_close_before_waiting(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_both_drain",
        "import m4a_test_gate\n"
        "CONTEXT = None\n"
        "class Clock:\n"
        "    async def now(self): return 7\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        global CONTEXT\n"
        "        CONTEXT = context\n"
        "        context.services.provide('clock', Clock())\n"
        "        async def listener(payload):\n"
        "            m4a_test_gate.ready.set()\n"
        "            await m4a_test_gate.release.wait()\n"
        "        context.events.subscribe('topic', listener)\n"
        "    async def start(self): return None\n"
        "    async def stop(self): return None\n",
    )
    item = definition(
        workspace,
        "worker",
        "m4a_test_both_drain",
        "events.publish",
        "events.subscribe",
        provides=(ProvidedService("clock", "1.0"),),
    )
    instance = host(workspace, item)

    async def scenario() -> None:
        gate = ModuleType("m4a_test_gate")
        gate.ready = asyncio.Event()
        gate.release = asyncio.Event()
        sys.modules["m4a_test_gate"] = gate
        await instance.activate()
        context = sys.modules["m4a_test_both_drain"].CONTEXT
        pending = asyncio.create_task(context.events.publish("topic", None))
        await gate.ready.wait()
        stopping = asyncio.create_task(instance.stop())
        while instance.get_state("worker") is not FiberState.UNLOADING:
            await asyncio.sleep(0)
        assert not instance._services._current["worker"].accepting
        assert not stopping.done()
        with pytest.raises(PluginActivationError):
            await context.events.publish("topic", None)
        gate.release.set()
        await pending
        await stopping

    run(scenario())


def test_old_context_is_revoked_after_stop_and_reactivation(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_restart",
        "CONTEXTS = []\n"
        "SEEN = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        CONTEXTS.append(context)\n"
        "        async def listener(payload): SEEN.append(payload)\n"
        "        context.events.subscribe('topic', listener)\n"
        "    async def start(self): return None\n"
        "    async def stop(self): return None\n",
    )
    item = definition(workspace, "worker", "m4a_test_restart", "events.publish", "events.subscribe")
    instance = host(workspace, item)

    async def scenario() -> None:
        await instance.activate()
        module = sys.modules["m4a_test_restart"]
        old = module.CONTEXTS[0]
        await old.events.publish("topic", 1)
        await instance.stop()
        with pytest.raises(PluginActivationError):
            await old.events.publish("topic", 2)
        await instance.activate()
        await module.CONTEXTS[1].events.publish("topic", 3)
        assert module.SEEN == [1, 3]
        await instance.stop()

    run(scenario())


def test_snapshot_skips_unsubscribed_and_new_subscriptions(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_snapshot",
        "import m4a_test_gate\n"
        "CONTEXT = None\n"
        "SECOND = None\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        global CONTEXT, SECOND\n"
        "        CONTEXT = context\n"
        "        async def first(payload):\n"
        "            m4a_test_gate.SEEN.append('first')\n"
        "            m4a_test_gate.ready.set()\n"
        "            await m4a_test_gate.release.wait()\n"
        "        async def second(payload): m4a_test_gate.SEEN.append('second')\n"
        "        context.events.subscribe('topic', first)\n"
        "        SECOND = context.events.subscribe('topic', second)\n"
        "    async def start(self): return None\n"
        "    async def stop(self): return None\n",
    )
    item = definition(
        workspace, "worker", "m4a_test_snapshot", "events.publish", "events.subscribe"
    )
    instance = host(workspace, item)

    async def scenario() -> None:
        gate = ModuleType("m4a_test_gate")
        gate.ready = asyncio.Event()
        gate.release = asyncio.Event()
        gate.SEEN = []
        sys.modules["m4a_test_gate"] = gate
        await instance.activate()
        module = sys.modules["m4a_test_snapshot"]
        pending = asyncio.create_task(module.CONTEXT.events.publish("topic", None))
        await gate.ready.wait()
        await module.SECOND.aclose()

        async def third(payload):
            gate.SEEN.append("third")

        module.CONTEXT.events.subscribe("topic", third)
        gate.release.set()
        await pending
        assert gate.SEEN == ["first"]
        await module.CONTEXT.events.publish("topic", None)
        assert gate.SEEN == ["first", "first", "third"]
        await instance.stop()

    run(scenario())


def test_listener_can_unsubscribe_itself_without_deadlock(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_self_unsubscribe",
        "CONTEXT = None\n"
        "SEEN = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        global CONTEXT\n"
        "        CONTEXT = context\n"
        "        async def listener(payload):\n"
        "            SEEN.append(payload)\n"
        "            await handle.aclose()\n"
        "        handle = context.events.subscribe('topic', listener)\n"
        "    async def start(self): return None\n"
        "    async def stop(self): return None\n",
    )
    item = definition(
        workspace, "worker", "m4a_test_self_unsubscribe", "events.publish", "events.subscribe"
    )
    instance = host(workspace, item)

    async def scenario() -> None:
        await instance.activate()
        module = sys.modules["m4a_test_self_unsubscribe"]
        await module.CONTEXT.events.publish("topic", 1)
        await module.CONTEXT.events.publish("topic", 2)
        assert module.SEEN == [1]
        await instance.stop()

    run(scenario())


def test_cancelled_publish_propagates_and_releases_inflight(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_cancel",
        "import asyncio\n"
        "import m4a_test_gate\n"
        "CONTEXT = None\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        global CONTEXT\n"
        "        CONTEXT = context\n"
        "        async def first(payload):\n"
        "            m4a_test_gate.ready.set()\n"
        "            try:\n"
        "                await asyncio.Event().wait()\n"
        "            except asyncio.CancelledError as exc:\n"
        "                m4a_test_gate.CANCELLED.append(exc)\n"
        "                raise\n"
        "        async def second(payload): m4a_test_gate.SEEN.append('second')\n"
        "        context.events.subscribe('topic', first)\n"
        "        context.events.subscribe('topic', second)\n"
        "    async def start(self): return None\n"
        "    async def stop(self): return None\n",
    )
    item = definition(workspace, "worker", "m4a_test_cancel", "events.publish", "events.subscribe")
    instance = host(workspace, item)

    async def scenario() -> None:
        gate = ModuleType("m4a_test_gate")
        gate.ready = asyncio.Event()
        gate.SEEN = []
        gate.CANCELLED = []
        gate.PROPAGATED = []
        sys.modules["m4a_test_gate"] = gate
        await instance.activate()
        context = sys.modules["m4a_test_cancel"].CONTEXT

        async def observed_publish() -> None:
            try:
                await context.events.publish("topic", None)
            except asyncio.CancelledError as exc:
                gate.PROPAGATED.append(exc)
                raise

        pending = asyncio.create_task(observed_publish())
        await gate.ready.wait()
        pending.cancel("caller-cancelled")
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert len(gate.CANCELLED) == len(gate.PROPAGATED) == 1
        assert gate.PROPAGATED[0] is gate.CANCELLED[0]
        assert gate.PROPAGATED[0].args == ("caller-cancelled",)
        assert gate.SEEN == []
        await instance.stop()
        assert instance.get_state("worker") is FiberState.PENDING

    run(scenario())


def test_reentrant_publish_is_not_blocked_by_a_global_lock(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_nested",
        "CONTEXT = None\n"
        "SEEN = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        global CONTEXT\n"
        "        CONTEXT = context\n"
        "        async def outer(payload):\n"
        "            SEEN.append('outer-enter')\n"
        "            await context.events.publish('inner', payload)\n"
        "            SEEN.append('outer-exit')\n"
        "        async def inner(payload): SEEN.append('inner')\n"
        "        context.events.subscribe('outer', outer)\n"
        "        context.events.subscribe('inner', inner)\n"
        "    async def start(self): return None\n"
        "    async def stop(self): return None\n",
    )
    item = definition(workspace, "worker", "m4a_test_nested", "events.publish", "events.subscribe")
    instance = host(workspace, item)

    async def scenario() -> None:
        await instance.activate()
        module = sys.modules["m4a_test_nested"]
        await module.CONTEXT.events.publish("outer", None)
        assert module.SEEN == ["outer-enter", "inner", "outer-exit"]
        await instance.stop()

    run(scenario())


def test_failed_start_discards_staged_subscriptions(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_failed",
        "CONTEXT = None\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        global CONTEXT\n"
        "        CONTEXT = context\n"
        "        async def listener(payload): return None\n"
        "        context.events.subscribe('topic', listener)\n"
        "    async def start(self): raise RuntimeError('start failed')\n"
        "    async def stop(self): return None\n",
    )
    item = definition(workspace, "worker", "m4a_test_failed", "events.publish", "events.subscribe")
    instance = host(workspace, item)

    async def scenario() -> None:
        with pytest.raises(PluginActivationError) as caught:
            await instance.activate()
        assert isinstance(caught.value.cause, RuntimeError)
        assert instance.get_state("worker") is FiberState.FAILED
        assert instance._events._topics == {}
        assert instance._events._owners == {}
        with pytest.raises(PluginActivationError):
            await sys.modules["m4a_test_failed"].CONTEXT.events.publish("topic", None)

    run(scenario())


def test_cancelled_activation_discards_staged_subscriptions(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_cancel_start",
        "import asyncio\n"
        "import m4a_test_gate\n"
        "CONTEXT = None\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        global CONTEXT\n"
        "        CONTEXT = context\n"
        "        async def listener(payload): m4a_test_gate.SEEN.append('event')\n"
        "        context.events.subscribe('topic', listener)\n"
        "    async def start(self):\n"
        "        m4a_test_gate.ready.set()\n"
        "        try:\n"
        "            await m4a_test_gate.release.wait()\n"
        "        except asyncio.CancelledError as exc:\n"
        "            m4a_test_gate.CANCELLED.append(exc)\n"
        "            raise\n"
        "    async def stop(self): m4a_test_gate.SEEN.append('stop')\n",
    )
    item = definition(
        workspace, "worker", "m4a_test_cancel_start", "events.publish", "events.subscribe"
    )
    instance = host(workspace, item)

    async def scenario() -> None:
        gate = ModuleType("m4a_test_gate")
        gate.ready = asyncio.Event()
        gate.release = asyncio.Event()
        gate.SEEN = []
        gate.CANCELLED = []
        gate.PROPAGATED = []
        sys.modules["m4a_test_gate"] = gate

        async def observed_activation() -> None:
            try:
                await instance.activate()
            except asyncio.CancelledError as exc:
                gate.PROPAGATED.append(exc)
                raise

        activation = asyncio.create_task(observed_activation())
        await gate.ready.wait()
        activation.cancel("activation-cancelled")
        with pytest.raises(asyncio.CancelledError):
            await activation
        assert len(gate.CANCELLED) == len(gate.PROPAGATED) == 1
        assert gate.PROPAGATED[0] is gate.CANCELLED[0]
        assert gate.PROPAGATED[0].args == ("activation-cancelled",)
        assert gate.SEEN == ["stop"]
        assert instance.get_state("worker") is FiberState.FAILED
        assert instance._events._topics == {}
        assert instance._events._owners == {}
        with pytest.raises(PluginActivationError):
            await sys.modules["m4a_test_cancel_start"].CONTEXT.events.publish("topic", None)

    run(scenario())


def test_unauthorized_subscribe_during_setup_fails_with_identity(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_unauthorized_setup",
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        async def listener(payload): return None\n"
        "        context.events.subscribe('topic', listener)\n"
        "    async def start(self): return None\n"
        "    async def stop(self): raise AssertionError('setup did not complete')\n",
    )
    item = definition(workspace, "worker", "m4a_test_unauthorized_setup")
    instance = host(workspace, item)

    async def scenario() -> None:
        with pytest.raises(PluginActivationError) as caught:
            await instance.activate()
        assert caught.value.phase == "setup"
        assert isinstance(caught.value.cause, CapabilityDeniedError)
        assert caught.value.cause.instance_id == "worker"
        assert caught.value.cause.missing == ("events.subscribe",)
        assert instance.get_state("worker") is FiberState.FAILED
        assert instance._events._topics == {}
        assert instance._events._owners == {}

    run(scenario())


def test_hosts_do_not_share_event_subscriptions(workspace: Path) -> None:
    plugin(
        workspace,
        "m4a_test_isolation",
        "CONTEXTS = []\n"
        "SEEN = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        CONTEXTS.append(context)\n"
        "        async def listener(payload): SEEN.append((context.instance_id, payload))\n"
        "        context.events.subscribe('topic', listener)\n"
        "    async def start(self): return None\n"
        "    async def stop(self): return None\n",
    )
    first = definition(
        workspace, "first", "m4a_test_isolation", "events.publish", "events.subscribe"
    )
    second = definition(
        workspace, "second", "m4a_test_isolation", "events.publish", "events.subscribe"
    )
    first_host = host(workspace, first)
    second_host = host(workspace, second)

    async def scenario() -> None:
        await first_host.activate()
        await second_host.activate()
        module = sys.modules["m4a_test_isolation"]
        await module.CONTEXTS[0].events.publish("topic", 7)
        assert module.SEEN == [("first", 7)]
        await first_host.stop()
        await second_host.stop()

    run(scenario())
