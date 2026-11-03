"""Run the manifest, service, event, and cleanup example."""

import asyncio
from pathlib import Path

from soul_plugin_core.capabilities import CapabilityPolicy
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.host import PluginHost
from soul_plugin_core.lifecycle import FiberState
from soul_plugin_core.manifest import load_manifest


def make_host() -> PluginHost:
    root = Path(__file__).resolve().parent
    definitions = tuple(
        load_manifest(root / "manifests" / name / "plugin.toml")
        for name in ("clock", "reporter", "emitter")
    )
    policy = CapabilityPolicy(
        {
            "demo.reporter": {"events.subscribe"},
            "demo.emitter": {"events.publish"},
        }
    )
    return PluginHost(
        definitions,
        build_activation_plan(definitions),
        config_path=root / "config.toml",
        capability_policy=policy,
    )


async def main() -> None:
    host = make_host()
    try:
        await host.activate()
        print("host: active")
    finally:
        await host.dispose()
    for instance_id in ("demo.clock", "demo.reporter", "demo.emitter"):
        if host.get_state(instance_id) is not FiberState.DISPOSED:
            raise RuntimeError(f"instance did not dispose: {instance_id}")
    print("host: disposed")


if __name__ == "__main__":
    asyncio.run(main())
