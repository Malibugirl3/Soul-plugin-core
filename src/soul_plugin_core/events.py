"""Host 内的非持久事件通知；订阅属于一次插件激活。"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from soul_plugin_core.errors import (
    CapabilityDeniedError,
    EventDispatchError,
    EventListenerFailure,
    PluginActivationError,
)

if TYPE_CHECKING:
    from soul_plugin_core.context import ActivationScope, EffectHandle

EventHandler = Callable[[object], Awaitable[object]]


@dataclass(eq=False)
class _Activation:
    plugin_id: str
    instance_id: str
    source: str
    scope: ActivationScope
    capabilities: frozenset[str]
    state: Literal["staged", "active", "draining", "revoked"] = "staged"
    inflight: int = 0
    idle: asyncio.Event = field(default_factory=asyncio.Event)
    subscriptions: dict[int, _Subscription] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.idle.set()

    def enter(self) -> None:
        self.inflight += 1
        self.idle.clear()

    def leave(self) -> None:
        self.inflight -= 1
        if self.inflight == 0:
            self.idle.set()


@dataclass(eq=False)
class _Subscription:
    number: int
    name: str
    handler: EventHandler
    owner: _Activation
    active: bool = True


class EventRegistry:
    """每个 Host 一份；同一发布的监听器按登记顺序串行执行。"""

    def __init__(self) -> None:
        self._owners: dict[str, _Activation] = {}
        self._topics: dict[str, dict[int, _Subscription]] = {}
        self._snapshots: dict[str, tuple[_Subscription, ...]] = {}
        self._next_number = 0

    def open_activation(
        self,
        plugin_id: str,
        instance_id: str,
        source: str,
        scope: ActivationScope,
        capabilities: frozenset[str],
    ) -> EventFacade:
        if instance_id in self._owners:
            raise RuntimeError("event activation is already open")
        owner = _Activation(plugin_id, instance_id, source, scope, capabilities)
        scope.defer(lambda: self._revoke(owner), label="event-activation")
        self._owners[instance_id] = owner
        return EventFacade(self, owner)

    def commit(self, instance_id: str) -> None:
        owner = self._owners[instance_id]
        if owner.state != "staged":
            raise RuntimeError("event activation cannot be committed")
        owner.state = "active"
        for subscription in owner.subscriptions.values():
            if subscription.active:
                self._activate(subscription)

    def begin_drain(self, instance_id: str) -> None:
        owner = self._owners.get(instance_id)
        if owner is None or owner.state in ("draining", "revoked"):
            return
        owner.state = "draining"
        for subscription in tuple(owner.subscriptions.values()):
            self._unsubscribe(subscription)

    async def drain(self, instance_id: str) -> None:
        self.begin_drain(instance_id)
        owner = self._owners.get(instance_id)
        if owner is not None:
            await owner.idle.wait()

    def revoke(self, instance_id: str) -> None:
        owner = self._owners.get(instance_id)
        if owner is not None:
            self._revoke(owner)

    def _revoke(self, owner: _Activation) -> None:
        for subscription in tuple(owner.subscriptions.values()):
            self._unsubscribe(subscription)
        owner.state = "revoked"
        if self._owners.get(owner.instance_id) is owner:
            del self._owners[owner.instance_id]

    def _subscribe(self, owner: _Activation, name: str, handler: EventHandler) -> EffectHandle:
        self._next_number += 1
        subscription = _Subscription(self._next_number, name, handler, owner)
        handle = owner.scope.defer(lambda: self._unsubscribe(subscription), label=f"event:{name}")
        owner.subscriptions[subscription.number] = subscription
        if owner.state == "active":
            self._activate(subscription)
        return handle

    def _activate(self, subscription: _Subscription) -> None:
        self._topics.setdefault(subscription.name, {})[subscription.number] = subscription
        self._snapshots.pop(subscription.name, None)

    def _unsubscribe(self, subscription: _Subscription) -> None:
        if not subscription.active:
            return
        subscription.active = False
        subscription.owner.subscriptions.pop(subscription.number, None)
        listeners = self._topics.get(subscription.name)
        if listeners is not None:
            listeners.pop(subscription.number, None)
            self._snapshots.pop(subscription.name, None)
            if not listeners:
                del self._topics[subscription.name]

    async def _publish(self, publisher: _Activation, name: str, payload: object) -> None:
        failures: list[EventListenerFailure] = []
        listeners = self._snapshots.get(name)
        if listeners is None:
            topic = self._topics.get(name)
            if topic:
                listeners = tuple(sorted(topic.values(), key=lambda item: item.number))
                self._snapshots[name] = listeners
            else:
                listeners = ()
        for subscription in listeners:
            owner = subscription.owner
            if not subscription.active or owner.state != "active":
                continue
            owner.enter()
            try:
                result = subscription.handler(payload)
                if not inspect.isawaitable(result):
                    raise TypeError("event listener must return an awaitable")
                await result
            except Exception as exc:  # noqa: BLE001 - 单项失败不阻止其余监听器
                failures.append(EventListenerFailure(owner.plugin_id, owner.instance_id, exc))
            finally:
                owner.leave()
        if failures:
            raise EventDispatchError(
                event=name,
                plugin_id=publisher.plugin_id,
                instance_id=publisher.instance_id,
                source=publisher.source,
                failures=failures,
            )


class EventFacade:
    """特定 Activation 的事件访问；权限和生命周期均按实例检查。"""

    def __init__(self, registry: EventRegistry, owner: _Activation) -> None:
        self._registry = registry
        self._owner = owner

    def subscribe(self, name: str, handler: EventHandler) -> EffectHandle:
        """订阅事件并返回撤销句柄；setup 中的订阅在启动成功后生效。"""
        self._ensure_open("events.subscribe")
        _check_name(name)
        if not callable(handler):
            raise TypeError("event listener must be callable")
        return self._registry._subscribe(self._owner, name, handler)

    async def publish(self, name: str, payload: object) -> None:
        """按顺序等待监听器；普通监听器错误汇总为 EventDispatchError。"""
        self._ensure_open("events.publish")
        _check_name(name)
        self._owner.enter()
        try:
            await self._registry._publish(self._owner, name, payload)
        finally:
            self._owner.leave()

    def _ensure_open(self, capability: str) -> None:
        owner = self._owner
        if owner.state not in ("staged", "active"):
            raise PluginActivationError(
                "event activation is no longer available",
                plugin_id=owner.plugin_id,
                instance_id=owner.instance_id,
                phase="events",
                source=owner.source,
            )
        if capability not in owner.capabilities:
            raise CapabilityDeniedError(
                plugin_id=owner.plugin_id,
                instance_id=owner.instance_id,
                source=owner.source,
                missing=(capability,),
            )


def _check_name(name: str) -> None:
    if not isinstance(name, str):
        raise TypeError("event name must be a string")
    if not name or name != name.strip():
        raise ValueError("event name must be non-empty without surrounding whitespace")
