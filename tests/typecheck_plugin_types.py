"""Static examples for the public plugin protocol and provider helper."""

from collections.abc import Mapping
from typing import Protocol

from soul_plugin_core.context import PluginContext
from soul_plugin_core.plugins import BasePlugin, PluginProtocol, ServicePlugin


class Clock(Protocol):
    async def now(self) -> int: ...


class ClockImpl:
    async def now(self) -> int:
        return 42


class ClockProvider(ServicePlugin[Clock]):
    service_name = "clock"

    async def create_service(self, context: PluginContext, config: Mapping[str, object]) -> Clock:
        return ClockImpl()


class ExistingPlugin:
    async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


async def accepts_protocol(plugin: PluginProtocol, context: PluginContext) -> None:
    await plugin.setup(context, context.config)


base_plugin: PluginProtocol = BasePlugin()
provider_plugin: PluginProtocol = ClockProvider()
plain_plugin: PluginProtocol = ExistingPlugin()
