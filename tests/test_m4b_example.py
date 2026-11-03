"""The public three-plugin demo runs independently of the working directory."""

import os
import subprocess
import sys
from pathlib import Path

import soul_plugin_core
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.manifest import load_manifest

DEMO = Path(__file__).resolve().parents[1] / "examples" / "m4_demo"


def test_demo_manifests_define_the_service_and_plugin_order() -> None:
    definitions = tuple(
        load_manifest(DEMO / "manifests" / name / "plugin.toml")
        for name in ("emitter", "clock", "reporter")
    )
    plan = build_activation_plan(definitions)
    assert plan.order == ("demo.clock", "demo.reporter", "demo.emitter")
    assert any(
        binding.consumer_id == "demo.reporter"
        and binding.kind == "service"
        and binding.provider_id == "demo.clock"
        for binding in plan.bindings
    )


def test_demo_runs_and_cleans_up_from_another_directory(tmp_path: Path) -> None:
    env = os.environ.copy()
    assert soul_plugin_core.__file__ is not None
    # The subprocess must use the same installed distribution as this test,
    # even when it was installed into an isolated --target directory.
    package_root = str(Path(soul_plugin_core.__file__).resolve().parent.parent)
    inherited_paths = env.get("PYTHONPATH")
    env["PYTHONPATH"] = os.pathsep.join(
        [package_root, inherited_paths] if inherited_paths else [package_root]
    )
    result = subprocess.run(
        [sys.executable, str(DEMO / "run_demo.py")],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert result.stdout.splitlines() == [
        "clock: start",
        "reporter: start",
        "reporter: hello from plugin @ 12:00",
        "emitter: start",
        "host: active",
        "emitter: stop",
        "reporter: stop",
        "reporter: cleanup",
        "clock: stop",
        "host: disposed",
    ]
