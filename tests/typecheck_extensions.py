"""静态检查插件作者可使用的观察型扩展接口。"""

from soul_plugin_core.context import EffectHandle, PluginContext


async def observe(payload: object) -> None:
    _ = payload


async def use_extensions(context: PluginContext) -> EffectHandle:
    handle = context.extensions.register("runtime.changed", observe)
    await context.extensions.invoke("runtime.changed", {"id": 1})
    return handle
