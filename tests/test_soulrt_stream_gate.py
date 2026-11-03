"""A streamed callback remains an in-flight service call until it returns."""

import asyncio
import sys
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from types import ModuleType
from typing import Protocol

import pytest

from soul_plugin_core.context import PluginContext
from soul_plugin_core.contracts import PluginDefinition, ServiceDependency
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import ServiceUnavailableError
from soul_plugin_core.host import PluginHost
from soul_plugin_core.lifecycle import FiberState
from soul_plugin_core.manifest import load_manifest

PROBE = Path(__file__).resolve().parents[1] / "examples" / "soulrt_probe"
Sink = Callable[[str], Awaitable[None]]


class Model(Protocol):
    async def stream(self, value: int, mode: str, emit: Sink) -> str: ...


def test_cached_stream_method_drains_and_then_becomes_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.syspath_prepend(str(PROBE))
    monkeypatch.delitem(sys.modules, "probe_plugins", raising=False)
    captured: list[Callable[[int, str, Sink], Awaitable[str]]] = []

    class Consumer:
        async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
            captured.append(context.services.require("probe.model", Model).service.stream)

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

    module = ModuleType("soulrt_stream_consumer")
    module.Consumer = Consumer  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    model = load_manifest(PROBE / "manifests" / "model" / "plugin.toml")
    consumer = PluginDefinition(
        plugin_id="probe.consumer",
        version="1.0",
        api_version="1",
        entry_point="soulrt_stream_consumer:Consumer",
        plugin_dependencies=(),
        service_dependencies=(ServiceDependency("probe.model", ">=1,<2", False),),
        provided_services=(),
        capability_requests=(),
        config_schema=None,
        source=str(tmp_path / "plugin.toml"),
    )
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    host = PluginHost(
        (model, consumer), build_activation_plan((model, consumer)), config_path=config
    )

    async def scenario() -> None:
        await host.activate()
        entered, release = asyncio.Event(), asyncio.Event()

        async def sink(_chunk: str) -> None:
            entered.set()
            await release.wait()

        call = asyncio.ensure_future(captured[0](42, "normal", sink))
        await asyncio.wait_for(entered.wait(), timeout=5)
        stopping = asyncio.create_task(host.stop())

        async def provider_unloading() -> None:
            while host.get_state("probe.model") is not FiberState.UNLOADING:
                await asyncio.sleep(0)

        try:
            await asyncio.wait_for(provider_unloading(), timeout=5)
            assert not stopping.done()
            with pytest.raises(ServiceUnavailableError):
                await captured[0](0, "normal", sink)
            release.set()
            assert await asyncio.wait_for(call, timeout=5) == "42"
            await asyncio.wait_for(stopping, timeout=5)
            with pytest.raises(ServiceUnavailableError):
                await captured[0](0, "normal", sink)
        finally:
            release.set()
            await asyncio.gather(call, stopping, return_exceptions=True)
            await host.dispose()

    try:
        asyncio.run(asyncio.wait_for(scenario(), timeout=15))
    finally:
        sys.modules.pop("probe_plugins", None)
