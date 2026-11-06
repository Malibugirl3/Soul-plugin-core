"""公开 Manifest 解析接口的静态类型回归。"""

from pathlib import Path

from typing_extensions import assert_type

from soul_plugin_core.contracts import PluginDefinition
from soul_plugin_core.manifest import load_manifest

assert_type(load_manifest("plugin.toml"), PluginDefinition)
assert_type(load_manifest(Path("plugin.toml")), PluginDefinition)
