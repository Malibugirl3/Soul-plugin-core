"""A plugin that provides a typed clock service."""

from collections.abc import Mapping

from soul_plugin_core.context import PluginContext


class ClockService:
    async def now(self) -> str:
        return "12:00"


class ClockPlugin:
    async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
        context.services.provide("demo.clock", ClockService())

    async def start(self) -> None:
        print("clock: start")

    async def stop(self) -> None:
        print("clock: stop")
