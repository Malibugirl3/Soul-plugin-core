"""通用默认插件及其惰性 Manifest 定位。"""

from pathlib import Path


def memory_kv_manifest() -> Path:
    """返回内存键值插件的 Manifest 路径，不导入插件入口。"""
    return Path(__file__).with_name("memory_kv.toml")
