"""Fiber 状态与入口加载。"""

import importlib
import inspect
from collections.abc import Awaitable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Protocol, cast

from soul_plugin_core.context import ActivationScope
from soul_plugin_core.contracts import PluginDefinition
from soul_plugin_core.errors import PluginLoadError


class PluginInstance(Protocol):
    def setup(self, context: object, config: object) -> Awaitable[object]: ...

    def start(self) -> Awaitable[object]: ...

    def stop(self) -> Awaitable[object]: ...


class FiberState(Enum):
    """诊断状态。底层值不是稳定序列化协议。"""

    PENDING = auto()
    LOADING = auto()
    ACTIVE = auto()
    UNLOADING = auto()
    FAILED = auto()
    DISPOSED = auto()


@dataclass
class Fiber:
    instance_id: str
    plugin_id: str
    source: str
    state: FiberState = FiberState.PENDING
    instance: PluginInstance | None = None
    scope: ActivationScope | None = None
    setup_completed: bool = False


def load_plugin(
    definition: PluginDefinition,
    instance_id: str | None = None,
) -> PluginInstance:
    """只接受无参类，并且 setup/start/stop 必须是异步函数。"""
    resolved_id = definition.plugin_id if instance_id is None else instance_id
    module_name, separator, class_name = definition.entry_point.partition(":")
    if separator == "" or module_name == "" or class_name == "":
        raise _load_error(definition, resolved_id, "entry point must be module:ClassName")
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise _load_error(definition, resolved_id, "cannot import plugin module", exc) from exc
    try:
        plugin_cls = getattr(module, class_name)
    except AttributeError as exc:
        raise _load_error(definition, resolved_id, "plugin class was not found", exc) from exc
    if not isinstance(plugin_cls, type):
        raise _load_error(definition, resolved_id, "plugin entry must be a class")
    try:
        instance = plugin_cls()
    except Exception as exc:
        raise _load_error(definition, resolved_id, "plugin construction failed", exc) from exc
    for hook in ("setup", "start", "stop"):
        if not inspect.iscoroutinefunction(getattr(instance, hook, None)):
            raise _load_error(definition, resolved_id, f"{hook} must be an async function")
    return cast(PluginInstance, instance)


def _load_error(
    definition: PluginDefinition,
    instance_id: str,
    message: str,
    cause: BaseException | None = None,
) -> PluginLoadError:
    return PluginLoadError(
        message,
        plugin_id=definition.plugin_id,
        instance_id=instance_id,
        phase="load",
        source=definition.source,
        cause=cause,
    )
