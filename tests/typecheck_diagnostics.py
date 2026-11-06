"""Static consumer example for Host deployment and instance snapshots."""

from soul_plugin_core.host import InstanceSnapshot, PluginHost
from soul_plugin_core.instances import InstanceActivationPlan
from soul_plugin_core.lifecycle import FiberState


def collect(host: PluginHost) -> tuple[InstanceSnapshot, ...]:
    plan: InstanceActivationPlan = host.instance_plan
    result: tuple[InstanceSnapshot, ...] = host.snapshot()
    for item in result:
        state: FiberState = item.state
        blockers: tuple[str, ...] = item.binding_blockers
        print(state, blockers, plan.order)
    return result
