"""把插件级激活计划展开成实例级计划。不读取配置，也不导入插件。"""

import heapq
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Literal

from soul_plugin_core.contracts import PluginDefinition
from soul_plugin_core.dependency import ActivationPlan, UnboundDependency
from soul_plugin_core.errors import InstanceResolutionError

_KINDS = frozenset({"plugin", "service"})
SelectionKey = tuple[str, str]


@dataclass(frozen=True)
class InstanceBindingSelection:
    """为消费者的一条依赖显式选择 Provider 实例。"""

    kind: Literal["plugin", "service"]
    target: str
    provider_instance_id: str


@dataclass(frozen=True)
class InstanceDefinition:
    """部署中的一个插件实例，以及需要显式指定的依赖绑定。"""

    instance_id: str
    plugin_id: str
    bindings: tuple[InstanceBindingSelection, ...] = ()


@dataclass(frozen=True)
class InstanceBinding:
    consumer_instance_id: str
    kind: str
    target: str
    provider_instance_id: str


@dataclass(frozen=True)
class InstanceUnboundDependency:
    consumer_instance_id: str
    kind: str
    target: str
    reason: str
    cycle: tuple[str, ...] = ()


@dataclass(frozen=True)
class InstanceActivationPlan:
    order: tuple[str, ...]
    bindings: tuple[InstanceBinding, ...]
    unbound: tuple[InstanceUnboundDependency, ...]
    plugin_ids: tuple[tuple[str, str], ...] = ()


def expand_instance_plan(
    definitions: Iterable[PluginDefinition],
    plugin_plan: ActivationPlan,
    instances: Iterable[InstanceDefinition] | None,
) -> InstanceActivationPlan:
    """展开实例计划。instances 为 None 时每个插件得到一个同名默认实例。"""
    by_id = {definition.plugin_id: definition for definition in definitions}
    declared = _declare_instances(by_id, plugin_plan, instances)
    providers = _providers_by_plugin(declared)
    binding_index = {
        (item.consumer_id, item.kind, item.target): item.provider_id
        for item in plugin_plan.bindings
    }
    unbound_index: dict[tuple[str, str, str], UnboundDependency] = {
        (item.consumer_id, item.kind, item.target): item for item in plugin_plan.unbound
    }
    produced: list[InstanceBinding] = []
    unbound: list[InstanceUnboundDependency] = []
    graph: dict[str, set[str]] = {item.instance_id: set() for item in declared}

    for item in sorted(declared, key=lambda declaration: declaration.instance_id):
        choices = _selections(item, binding_index, unbound_index)
        _expand_bindings(
            item,
            by_id[item.plugin_id],
            choices,
            binding_index,
            providers,
            produced,
            unbound,
            graph,
        )
        _copy_unbound(item, unbound_index, unbound)

    order = _topological_order(graph)
    if len(order) != len(graph):
        raise _error("instance dependency cycle", field="order")
    return InstanceActivationPlan(
        order,
        tuple(sorted(produced, key=_binding_key)),
        tuple(sorted(unbound, key=_unbound_key)),
        tuple(sorted((item.instance_id, item.plugin_id) for item in declared)),
    )


def _declare_instances(
    by_id: Mapping[str, PluginDefinition],
    plugin_plan: ActivationPlan,
    instances: Iterable[InstanceDefinition] | None,
) -> tuple[InstanceDefinition, ...]:
    if instances is None:
        return tuple(InstanceDefinition(plugin_id, plugin_id) for plugin_id in plugin_plan.order)
    declared = tuple(instances)
    seen: set[str] = set()
    planned = set(plugin_plan.order)
    for item in declared:
        instance_id = item.instance_id
        if (
            not isinstance(instance_id, str)
            or instance_id != instance_id.strip()
            or not instance_id
        ):
            raise _error(
                "instance id must be a non-empty string without surrounding whitespace",
                plugin_id=item.plugin_id,
                instance_id=instance_id if isinstance(instance_id, str) else None,
                field="instance_id",
            )
        if instance_id in seen:
            raise _error(
                f"duplicate instance id {instance_id!r}",
                plugin_id=item.plugin_id,
                instance_id=instance_id,
                field="instance_id",
            )
        seen.add(instance_id)
        if item.plugin_id not in by_id or item.plugin_id not in planned:
            raise _error(
                f"unknown plugin {item.plugin_id!r}",
                plugin_id=item.plugin_id,
                instance_id=instance_id,
                field="plugin_id",
            )
    return declared


def _providers_by_plugin(
    declared: tuple[InstanceDefinition, ...],
) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for item in declared:
        grouped.setdefault(item.plugin_id, []).append(item.instance_id)
    return {plugin_id: tuple(sorted(ids)) for plugin_id, ids in grouped.items()}


def _selections(
    item: InstanceDefinition,
    binding_index: Mapping[tuple[str, str, str], str],
    unbound_index: Mapping[tuple[str, str, str], UnboundDependency],
) -> dict[SelectionKey, str]:
    chosen: dict[SelectionKey, str] = {}
    for selection in item.bindings:
        key = (selection.kind, selection.target)
        if selection.kind not in _KINDS:
            raise _error(
                "selection kind must be plugin or service",
                plugin_id=item.plugin_id,
                instance_id=item.instance_id,
                field="kind",
                path=key,
            )
        if key in chosen:
            raise _error(
                "duplicate provider selection",
                plugin_id=item.plugin_id,
                instance_id=item.instance_id,
                field=_field(key),
                path=key,
            )
        plugin_key = (item.plugin_id, selection.kind, selection.target)
        if plugin_key in unbound_index:
            raise _error(
                "selection cannot restore an unbound optional dependency",
                plugin_id=item.plugin_id,
                instance_id=item.instance_id,
                field=_field(key),
                path=key,
            )
        if plugin_key not in binding_index:
            raise _error(
                "selection does not match a resolved dependency",
                plugin_id=item.plugin_id,
                instance_id=item.instance_id,
                field=_field(key),
                path=key,
            )
        chosen[key] = selection.provider_instance_id
    return chosen


def _expand_bindings(
    item: InstanceDefinition,
    definition: PluginDefinition,
    choices: Mapping[SelectionKey, str],
    binding_index: Mapping[tuple[str, str, str], str],
    providers: Mapping[str, tuple[str, ...]],
    produced: list[InstanceBinding],
    unbound: list[InstanceUnboundDependency],
    graph: dict[str, set[str]],
) -> None:
    consumer_bindings = sorted(
        (kind, target)
        for consumer_id, kind, target in binding_index
        if consumer_id == item.plugin_id
    )
    for kind, target in consumer_bindings:
        selection_key = (kind, target)
        provider_plugin = binding_index[(item.plugin_id, kind, target)]
        provider_instances = providers.get(provider_plugin, ())
        chosen = choices.get(selection_key)
        if chosen is not None:
            _require_choice(item, selection_key, chosen, provider_instances)
            _bind(item.instance_id, selection_key, chosen, produced, graph)
            continue
        if len(provider_instances) == 1:
            _bind(item.instance_id, selection_key, provider_instances[0], produced, graph)
            continue
        if len(provider_instances) == 0:
            if _is_optional(definition, kind, target):
                unbound.append(InstanceUnboundDependency(item.instance_id, kind, target, "missing"))
                continue
            raise _error(
                "provider plugin has no instance",
                plugin_id=item.plugin_id,
                instance_id=item.instance_id,
                field=_field(selection_key),
                path=selection_key,
            )
        raise _error(
            "provider plugin has multiple instances",
            plugin_id=item.plugin_id,
            instance_id=item.instance_id,
            field=_field(selection_key),
            path=selection_key,
        )


def _copy_unbound(
    item: InstanceDefinition,
    unbound_index: Mapping[tuple[str, str, str], UnboundDependency],
    unbound: list[InstanceUnboundDependency],
) -> None:
    for key, record in sorted(unbound_index.items()):
        consumer_id, kind, target = key
        if consumer_id != item.plugin_id:
            continue
        unbound.append(
            InstanceUnboundDependency(
                item.instance_id,
                kind,
                target,
                record.reason,
                record.cycle,
            )
        )


def _require_choice(
    item: InstanceDefinition,
    key: SelectionKey,
    provider_instance_id: str,
    provider_instances: tuple[str, ...],
) -> None:
    if provider_instance_id == item.instance_id or provider_instance_id not in provider_instances:
        raise _error(
            "selection does not name an instance of the provider plugin",
            plugin_id=item.plugin_id,
            instance_id=item.instance_id,
            field=_field(key),
            path=key,
        )


def _bind(
    consumer_instance_id: str,
    key: SelectionKey,
    provider_instance_id: str,
    produced: list[InstanceBinding],
    graph: dict[str, set[str]],
) -> None:
    kind, target = key
    produced.append(InstanceBinding(consumer_instance_id, kind, target, provider_instance_id))
    graph[provider_instance_id].add(consumer_instance_id)


def _is_optional(definition: PluginDefinition, kind: str, target: str) -> bool:
    if kind == "plugin":
        return any(
            item.plugin_id == target and item.optional for item in definition.plugin_dependencies
        )
    return any(item.name == target and item.optional for item in definition.service_dependencies)


def _topological_order(graph: Mapping[str, set[str]]) -> tuple[str, ...]:
    """Kahn 排序。每一轮取当前零入度实例里最小的 instance_id。"""
    pending = {node: 0 for node in graph}
    for consumers in graph.values():
        for consumer in consumers:
            pending[consumer] += 1
    ready = [node for node, count in pending.items() if count == 0]
    heapq.heapify(ready)
    order: list[str] = []
    while ready:
        node = heapq.heappop(ready)
        order.append(node)
        for consumer in sorted(graph[node]):
            pending[consumer] -= 1
            if pending[consumer] == 0:
                heapq.heappush(ready, consumer)
    return tuple(order)


def _binding_key(item: InstanceBinding) -> tuple[str, str, str, str]:
    return (item.consumer_instance_id, item.kind, item.target, item.provider_instance_id)


def _unbound_key(
    item: InstanceUnboundDependency,
) -> tuple[str, str, str, str, tuple[str, ...]]:
    return (item.consumer_instance_id, item.kind, item.target, item.reason, item.cycle)


def _field(key: SelectionKey) -> str:
    return f"{key[0]}:{key[1]}"


def _error(
    message: str,
    *,
    plugin_id: str | None = None,
    instance_id: str | None = None,
    field: str | None = None,
    path: tuple[str, ...] = (),
) -> InstanceResolutionError:
    return InstanceResolutionError(
        message,
        plugin_id=plugin_id,
        instance_id=instance_id,
        field=field,
        path=path,
    )
