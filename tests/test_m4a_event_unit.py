"""Event registry boundaries without Host dependencies."""

import asyncio

import pytest

from soul_plugin_core.context import ActivationScope
from soul_plugin_core.errors import CapabilityDeniedError, EventDispatchError, PluginActivationError
from soul_plugin_core.events import EventRegistry


def test_staged_subscription_can_be_revoked_before_commit() -> None:
    async def scenario() -> None:
        registry = EventRegistry()
        scope = ActivationScope("worker", "worker")
        facade = registry.open_activation(
            "worker",
            "worker",
            "plugin.toml",
            scope,
            frozenset({"events.publish", "events.subscribe"}),
        )
        seen: list[object] = []

        async def listener(payload: object) -> None:
            seen.append(payload)

        handle = facade.subscribe("topic", listener)
        await handle.aclose()
        registry.commit("worker")
        await facade.publish("topic", 7)
        assert seen == []
        await registry.drain("worker")
        assert await scope.close() == ()
        assert registry._topics == {}
        assert registry._owners == {}

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_registration_order_is_preserved_across_staged_commit() -> None:
    async def scenario() -> None:
        registry = EventRegistry()
        first_scope = ActivationScope("first", "first")
        second_scope = ActivationScope("second", "second")
        first = registry.open_activation(
            "first",
            "first",
            "first.toml",
            first_scope,
            frozenset({"events.publish", "events.subscribe"}),
        )
        second = registry.open_activation(
            "second", "second", "second.toml", second_scope, frozenset({"events.subscribe"})
        )
        registry.commit("first")
        seen: list[str] = []

        async def early(payload: object) -> None:
            seen.append("staged-first")

        async def late(payload: object) -> None:
            seen.append("active-second")

        second.subscribe("topic", early)
        first.subscribe("topic", late)
        registry.commit("second")
        await first.publish("topic", None)
        assert seen == ["staged-first", "active-second"]
        await registry.drain("first")
        await registry.drain("second")
        assert await first_scope.close() == ()
        assert await second_scope.close() == ()
        assert registry._snapshots == {}

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_synchronous_listener_failure_is_aggregated_without_payload() -> None:
    async def scenario() -> None:
        registry = EventRegistry()
        scope = ActivationScope("worker", "worker")
        facade = registry.open_activation(
            "worker",
            "worker",
            "plugin.toml",
            scope,
            frozenset({"events.publish", "events.subscribe"}),
        )
        facade.subscribe("topic", lambda _: None)  # type: ignore[arg-type,return-value]
        registry.commit("worker")
        with pytest.raises(EventDispatchError) as caught:
            await facade.publish("topic", "secret payload")
        assert len(caught.value.failures) == 1
        assert isinstance(caught.value.errors[0], TypeError)
        assert caught.value.failures[0].instance_id == "worker"
        assert "secret payload" not in str(caught.value)
        await registry.drain("worker")
        assert await scope.close() == ()

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_invalid_event_names_and_revoked_facade_are_rejected() -> None:
    async def scenario() -> None:
        registry = EventRegistry()
        scope = ActivationScope("worker", "worker")
        facade = registry.open_activation(
            "worker",
            "worker",
            "plugin.toml",
            scope,
            frozenset({"events.publish", "events.subscribe"}),
        )

        async def listener(payload: object) -> None:
            return None

        for name in ("", " topic", "topic "):
            with pytest.raises(ValueError):
                facade.subscribe(name, listener)
            with pytest.raises(ValueError):
                await facade.publish(name, None)
        with pytest.raises(TypeError):
            facade.subscribe(1, listener)  # type: ignore[arg-type]
        registry.commit("worker")
        await registry.drain("worker")
        assert await scope.close() == ()
        with pytest.raises(PluginActivationError):
            facade.subscribe("topic", listener)
        with pytest.raises(PluginActivationError):
            await facade.publish("topic", None)

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_publish_and_subscribe_require_separate_capabilities() -> None:
    async def scenario() -> None:
        registry = EventRegistry()
        scope = ActivationScope("worker", "worker")
        facade = registry.open_activation(
            "worker", "worker", "plugin.toml", scope, frozenset({"events.subscribe"})
        )

        async def listener(payload: object) -> None:
            return None

        facade.subscribe("topic", listener)
        registry.commit("worker")
        with pytest.raises(CapabilityDeniedError) as caught:
            await facade.publish("topic", None)
        assert caught.value.missing == ("events.publish",)
        await registry.drain("worker")
        assert await scope.close() == ()

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))
