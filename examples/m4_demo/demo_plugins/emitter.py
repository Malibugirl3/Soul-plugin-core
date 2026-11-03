"""A plugin that publishes a single in-process event."""

from collections.abc import Mapping

from soul_plugin_core.context import PluginContext


class EmitterPlugin:
    def __init__(self) -> None:
        self._context: PluginContext | None = None

    async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
        self._context = context

    async def start(self) -> None:
        if self._context is None:
            raise RuntimeError("context is not available")
        await self._context.events.publish("demo.message", "hello from plugin")
        print("emitter: start")

    async def stop(self) -> None:
        print("emitter: stop")
