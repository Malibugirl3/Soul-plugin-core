"""The typed plugin example runs from an unrelated working directory."""

import os
import subprocess
import sys
from pathlib import Path

import soul_plugin_core

DEMO = Path(__file__).resolve().parents[1] / "examples" / "plugin_types_demo" / "run_demo.py"


def test_demo_uses_the_installed_core_and_disposes_every_instance(tmp_path: Path) -> None:
    env = os.environ.copy()
    assert soul_plugin_core.__file__ is not None
    package_root = str(Path(soul_plugin_core.__file__).resolve().parent.parent)
    inherited = env.get("PYTHONPATH")
    env["PYTHONPATH"] = os.pathsep.join([package_root, inherited] if inherited else [package_root])
    result = subprocess.run(
        [sys.executable, str(DEMO)],
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
        "plan: core.memory_kv, demo.normalizer, demo.workflow, demo.client",
        "workflow: SOULRT",
        "host: disposed",
    ]
