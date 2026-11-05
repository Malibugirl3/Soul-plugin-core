"""生命周期受管的观察型扩展点，与普通事件通道相互隔离。"""

from typing import TYPE_CHECKING

from soul_plugin_core.errors import CapabilityDeniedError
from soul_plugin_core.events import EventFacade, EventHandler, EventRegistry

if TYPE_CHECKING:
    from soul_plugin_core.context import ActivationScope, EffectHandle


class ExtensionFacade:
    """登记或调用观察者；返回值不参与替换或包装调用流程。"""

    def __init__(
        self,
        channel: EventFacade,
        plugin_id: str,
        instance_id: str,
        source: str,
        capabilities: frozenset[str],
    ) -> None:
        self._channel = channel
        self._plugin_id = plugin_id
        self._instance_id = instance_id
        self._source = source
        self._capabilities = capabilities

    def register(self, name: str, observer: EventHandler) -> "EffectHandle":
        """登记观察者；返回受 Scope 管理、可提前撤销的句柄。"""
        self._require("extensions.register")
        return self._channel.subscribe(name, observer)

    async def invoke(self, name: str, payload: object) -> None:
        """依登记顺序等待观察者；普通异常以 EventDispatchError 汇总。"""
        self._require("extensions.invoke")
        await self._channel.publish(name, payload)

    def _require(self, capability: str) -> None:
        if capability not in self._capabilities:
            raise CapabilityDeniedError(
                plugin_id=self._plugin_id,
                instance_id=self._instance_id,
                source=self._source,
                missing=(capability,),
            )


class ExtensionRegistry:
    """宿主内部注册表，复用事件排空机制但不共享事件通道或权限。"""

    def __init__(self) -> None:
        self._channel = EventRegistry()

    def open_activation(
        self,
        plugin_id: str,
        instance_id: str,
        source: str,
        scope: "ActivationScope",
        capabilities: frozenset[str],
    ) -> ExtensionFacade:
        # 这是私有通道的内部授权，外部调用必须先通过扩展自身的能力检查。
        channel = self._channel.open_activation(
            plugin_id, instance_id, source, scope, frozenset(("events.subscribe", "events.publish"))
        )
        return ExtensionFacade(channel, plugin_id, instance_id, source, capabilities)

    def commit(self, instance_id: str) -> None:
        self._channel.commit(instance_id)

    def begin_drain(self, instance_id: str) -> None:
        self._channel.begin_drain(instance_id)

    async def drain(self, instance_id: str) -> None:
        await self._channel.drain(instance_id)

    def revoke(self, instance_id: str) -> None:
        self._channel.revoke(instance_id)
