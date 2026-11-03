"""mypy 样例：能力策略与插件视图保持只读、类型明确。"""

from collections.abc import Mapping

from soul_plugin_core.capabilities import CapabilityPolicy
from soul_plugin_core.context import PluginContext


def _capabilities(context: PluginContext, policy: CapabilityPolicy) -> bool:
    granted: frozenset[str] = context.capabilities
    by_instance: Mapping[str, frozenset[str]] = policy.grants
    configured: frozenset[str] = policy.grants_for(context.instance_id)
    return "logging.write" in granted and configured == by_instance[context.instance_id]
