"""Typed strategy or backend selection keeps the selected service interface."""

from typing import Protocol

from soul_plugin_core.context import PluginContext
from soul_plugin_core.selection import ServiceSelector


class Policy(Protocol):
    async def choose(self, values: tuple[str, ...]) -> str: ...


async def use_policy(context: PluginContext) -> str:
    first = context.services.require("policy.first", Policy)
    last = context.services.require("policy.last", Policy)
    selector: ServiceSelector[Policy] = ServiceSelector({"first": first, "last": last})
    return await selector.select("last").service.choose(("a", "b"))
