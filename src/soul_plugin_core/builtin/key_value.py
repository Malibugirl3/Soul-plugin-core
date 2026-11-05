"""键值服务的独立类型；导入接口不加载具体插件入口。"""

from typing import Protocol


class KeyValueService(Protocol):
    """字符串键与值的最小异步接口。"""

    async def put(self, key: str, value: str) -> None: ...

    async def get(self, key: str) -> str | None: ...

    async def delete(self, key: str) -> bool: ...
