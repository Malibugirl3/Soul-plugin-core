"""A plugin that receives an event and calls the clock service."""

from collections.abc import Mapping
from typing import Protocol

from soul_plugin_core.context import PluginContext
from soul_plugin_core.services import ServiceHandle


class Clock(Protocol):
    async def now(self) -> str: ...


class ReporterPlugin:
    def __init__(self) -> None:
        self._clock: ServiceHandle[Clock] | None = None

    async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
        self._clock = context.services.require("demo.clock", Clock)
        context.effects.defer(lambda: print("reporter: cleanup"), label="demo-cleanup")
        context.events.subscribe("demo.message", self._on_message)

    async def _on_message(self, payload: object) -> None:
        if not isinstance(payload, str):
            raise TypeError("demo.message expects a string")
        clock = self._clock
        if clock is None:
            raise RuntimeError("clock is not available")
        print(f"reporter: {payload} @ {await clock.service.now()}")

    async def start(self) -> None:
        print("reporter: start")

    async def stop(self) -> None:
        print("reporter: stop")
