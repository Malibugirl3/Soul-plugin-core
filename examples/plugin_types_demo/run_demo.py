"""Compose the default store and typed example plugins from Manifest paths."""

import asyncio
from pathlib import Path

from soul_plugin_core.builtin import memory_kv_manifest
from soul_plugin_core.composition import PluginBundle, compose_plugins
from soul_plugin_core.lifecycle import FiberState

ROOT = Path(__file__).resolve().parent


async def main() -> None:
    bundle = PluginBundle.from_manifests(
        memory_kv_manifest(),
        *(
            ROOT / "manifests" / name / "plugin.toml"
            for name in ("normalizer", "workflow", "client")
        ),
    )
    composition = compose_plugins(bundle, config_path=ROOT / "config.toml")
    print("plan: " + ", ".join(composition.instance_plan.order))
    host = composition.create_host()
    try:
        await host.activate()
    finally:
        await host.dispose()
    if any(
        host.get_state(instance_id) is not FiberState.DISPOSED
        for instance_id in composition.instance_plan.order
    ):
        raise RuntimeError("an instance was not disposed")
    print("host: disposed")


if __name__ == "__main__":
    asyncio.run(main())
