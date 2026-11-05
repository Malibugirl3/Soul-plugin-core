"""可选插件协议与单服务提供者辅助类。"""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Generic, Protocol, TypeVar

from soul_plugin_core.context import PluginContext

T = TypeVar("T")


class PluginProtocol(Protocol):
    """插件生命周期的静态协议；现有插件无需继承它。"""

    async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


class BasePlugin:
    """可选生命周期基类；未覆盖的钩子无操作。"""

    async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
        """初始化插件，并在取得资源后立即登记清理。"""

    async def start(self) -> None:
        """启动插件；成功后由宿主提交其服务。"""

    async def stop(self) -> None:
        """停止插件；资源 Scope 随后由宿主关闭。"""


class ServicePlugin(BasePlugin, ABC, Generic[T]):
    """为 Manifest 中声明的单个服务创建并暂存实现。"""

    service_name: str

    @abstractmethod
    async def create_service(self, context: PluginContext, config: Mapping[str, object]) -> T:
        """创建服务；取得任何资源后应在 context.effects 中登记清理。"""

    async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
        """创建并暂存实现；服务版本由 Manifest 确定。"""
        implementation = await self.create_service(context, config)
        context.services.provide(self.service_name, implementation)
