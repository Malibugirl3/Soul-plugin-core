"""Static example for immutable bundles and a typed async pipeline."""

from pathlib import Path

from soul_plugin_core.capabilities import CapabilityPolicy
from soul_plugin_core.composition import PluginBundle, compose_plugins
from soul_plugin_core.host import PluginHost
from soul_plugin_core.pipeline import AsyncPipeline


async def normalize(value: str) -> str:
    return value.strip()


async def decorate(value: str) -> str:
    return value + "!"


async def process(value: str) -> str:
    pipeline: AsyncPipeline[str] = AsyncPipeline((normalize, decorate))
    return await pipeline.run(value)


def make_host(manifest: Path, config: Path, policy: CapabilityPolicy) -> PluginHost:
    bundle = PluginBundle.from_manifests(manifest)
    return compose_plugins(bundle, config_path=config, capability_policy=policy).create_host()
