"""按插件实例隔离的进程内、非持久化键值服务。"""

from collections.abc import Mapping

from soul_plugin_core.builtin.key_value import KeyValueService
from soul_plugin_core.context import PluginContext
from soul_plugin_core.plugins import ServicePlugin


def _check_string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


class _MemoryKeyValue:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}

    async def put(self, key: str, value: str) -> None:
        self._values[_check_string(key, "key")] = _check_string(value, "value")

    async def get(self, key: str) -> str | None:
        return self._values.get(_check_string(key, "key"))

    async def delete(self, key: str) -> bool:
        key = _check_string(key, "key")
        if key not in self._values:
            return False
        del self._values[key]
        return True

    def clear(self) -> None:
        self._values.clear()


class MemoryKeyValuePlugin(ServicePlugin[KeyValueService]):
    """提供 `core.kv`；实例关闭后清除本次激活的全部数据。"""

    service_name = "core.kv"

    async def create_service(
        self, context: PluginContext, config: Mapping[str, object]
    ) -> KeyValueService:
        service = _MemoryKeyValue()
        context.effects.defer(service.clear, label="memory-kv-clear")
        return service
