"""mypy 样例：事件门面与订阅句柄有明确类型。"""

from soul_plugin_core.context import EffectHandle, PluginContext


async def _listener(payload: object) -> None:
    if isinstance(payload, str):
        payload.upper()


async def _example(context: PluginContext) -> None:
    subscription: EffectHandle = context.events.subscribe("message", _listener)
    await context.events.publish("message", "hello")
    await subscription.aclose()
