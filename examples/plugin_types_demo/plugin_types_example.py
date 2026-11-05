"""Small typed plugins demonstrating services, a facade, and a pipeline."""

from collections.abc import Mapping
from typing import Protocol

from soul_plugin_core.builtin.key_value import KeyValueService
from soul_plugin_core.context import PluginContext
from soul_plugin_core.pipeline import AsyncPipeline
from soul_plugin_core.plugins import BasePlugin, ServicePlugin
from soul_plugin_core.services import ServiceHandle


class TextTransform(Protocol):
    async def apply(self, value: str) -> str: ...


class TextWorkflow(Protocol):
    async def run(self, value: str) -> str: ...


class _Normalizer:
    async def apply(self, value: str) -> str:
        return value.strip().upper()


class NormalizerPlugin(ServicePlugin[TextTransform]):
    service_name = "demo.normalizer"

    async def create_service(
        self, context: PluginContext, config: Mapping[str, object]
    ) -> TextTransform:
        return _Normalizer()


class WorkflowPlugin(BasePlugin):
    def __init__(self) -> None:
        self._pipeline: AsyncPipeline[str] | None = None

    async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
        normalizer = context.services.require("demo.normalizer", TextTransform)
        store = context.services.require("core.kv", KeyValueService)

        async def save(value: str) -> str:
            await store.service.put("latest", value)
            if await store.service.get("latest") != value:
                raise RuntimeError("stored value does not match")
            return value

        self._pipeline = AsyncPipeline((normalizer.service.apply, save))
        context.services.provide("demo.workflow", self)

    async def run(self, value: str) -> str:
        if self._pipeline is None:
            raise RuntimeError("workflow is not ready")
        return await self._pipeline.run(value)


class ClientPlugin(BasePlugin):
    def __init__(self) -> None:
        self._workflow: ServiceHandle[TextWorkflow] | None = None

    async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
        self._workflow = context.services.require("demo.workflow", TextWorkflow)

    async def start(self) -> None:
        if self._workflow is None:
            raise RuntimeError("client is not ready")
        result = await self._workflow.service.run("  SoulRT  ")
        print(f"workflow: {result}")
