"""Run a disposable integration probe; this is not the SoulRT implementation."""

import asyncio
import json
import sys
from pathlib import Path

from soul_plugin_core.capabilities import CapabilityPolicy
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.host import PluginHost
from soul_plugin_core.manifest import load_manifest

ROOT = Path(__file__).resolve().parent
PLUGIN_IDS = ("probe.model", "probe.tool", "probe.agent", "probe.entry")


def make_host() -> PluginHost:
    definitions = tuple(
        load_manifest(ROOT / "manifests" / name / "plugin.toml")
        for name in ("model", "tool", "agent", "entry")
    )
    return PluginHost(
        definitions,
        build_activation_plan(definitions),
        config_path=ROOT / "config.toml",
        capability_policy=CapabilityPolicy(
            {"probe.agent": {"events.publish"}, "probe.entry": {"events.subscribe"}}
        ),
    )


async def main() -> None:
    host = make_host()
    try:
        await host.activate()
        # The parent process controls graceful shutdown through stdin.
        await asyncio.to_thread(sys.stdin.readline)
    finally:
        await host.dispose()
    states = {plugin_id: host.get_state(plugin_id).name for plugin_id in PLUGIN_IDS}
    print(json.dumps({"type": "closed", "states": states}), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
