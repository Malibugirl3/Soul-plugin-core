"""Static type-checking example for the default key-value service."""

from soul_plugin_core.builtin.key_value import KeyValueService
from soul_plugin_core.context import PluginContext
from soul_plugin_core.services import ServiceHandle


async def use_store(context: PluginContext) -> str | None:
    handle: ServiceHandle[KeyValueService] = context.services.require("core.kv", KeyValueService)
    await handle.service.put("name", "Soul")
    deleted: bool = await handle.service.delete("unused")
    if deleted:
        return None
    return await handle.service.get("name")
