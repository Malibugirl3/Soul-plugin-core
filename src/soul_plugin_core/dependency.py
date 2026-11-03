import heapq
from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from packaging.specifiers import SpecifierSet
from packaging.version import Version

from soul_plugin_core.contracts import PluginDefinition, ProvidedService
from soul_plugin_core.errors import DependencyResolutionError


@dataclass(frozen=True)
class Binding:
    consumer_id: str
    kind: str
    target: str
    provider_id: str


@dataclass(frozen=True)
class UnboundDependency:
    consumer_id: str
    kind: str
    target: str
    reason: str
    cycle: tuple[str, ...] = ()


@dataclass(frozen=True)
class ActivationPlan:
    order: tuple[str, ...]
    bindings: tuple[Binding, ...]
    unbound: tuple[UnboundDependency, ...]


@dataclass(frozen=True)
class _Candidate:
    """已通过版本检查、尚未决定是否入图的一条依赖。"""

    consumer_id: str
    kind: str
    target: str
    provider_id: str
    optional: bool


def build_activation_plan(
    definitions: Iterable[PluginDefinition],
) -> ActivationPlan:
    """由已校验的定义生成不可变激活计划。不导入插件入口。"""
    definitions = tuple(definitions)
    by_id = _index_plugins(definitions)
    services = _index_services(by_id)
    _validate_self_dependencies(by_id)

    graph: dict[str, set[str]] = {plugin_id: set() for plugin_id in by_id}
    bindings: list[Binding] = []
    unbound: list[UnboundDependency] = []
    optional: list[_Candidate] = []

    for plugin_id in sorted(by_id):
        _collect_dependencies(
            by_id[plugin_id],
            by_id,
            services,
            graph,
            bindings,
            unbound,
            optional,
        )

    _require_acyclic(graph, by_id)
    _apply_optional(graph, bindings, unbound, optional)

    return ActivationPlan(
        _topological_order(graph),
        tuple(sorted(bindings, key=_binding_key)),
        tuple(sorted(unbound, key=_unbound_key)),
    )


def _index_plugins(
    definitions: Iterable[PluginDefinition],
) -> dict[str, PluginDefinition]:
    """建立插件 ID 索引，重复 ID 抛出依赖解析错误。"""
    by_id: dict[str, PluginDefinition] = {}

    for definition in sorted(
        definitions,
        key=lambda item: (item.plugin_id, item.source, item.version),
    ):
        plugin_id = definition.plugin_id

        if plugin_id in by_id:
            previous = by_id[plugin_id]
            raise DependencyResolutionError(
                f"duplicate plugin id {plugin_id!r}; first source={previous.source}",
                source=definition.source,
                plugin_id=plugin_id,
                field="plugin.id",
            )

        by_id[plugin_id] = definition

    return by_id


def _index_services(
    by_id: Mapping[str, PluginDefinition],
) -> dict[str, tuple[str, ProvidedService]]:
    """建立服务 Provider 索引，同域同名服务声明不允许覆盖。"""
    providers: dict[str, tuple[str, ProvidedService]] = {}

    for plugin_id in sorted(by_id):
        definition = by_id[plugin_id]

        for service in sorted(
            definition.provided_services,
            key=lambda item: item.name,
        ):
            if service.name in providers:
                previous_id, _ = providers[service.name]
                raise DependencyResolutionError(
                    f"duplicate service {service.name!r}; first provider={previous_id!r}",
                    source=definition.source,
                    plugin_id=plugin_id,
                    field="provides.services",
                )

            providers[service.name] = (plugin_id, service)

    return providers


def _validate_self_dependencies(
    by_id: Mapping[str, PluginDefinition],
) -> None:
    """拒绝显式自依赖；必须先于版本筛选和 Optional mask。"""
    for plugin_id in sorted(by_id):
        definition = by_id[plugin_id]

        for index, plugin_dependency in enumerate(definition.plugin_dependencies):
            if plugin_dependency.plugin_id == plugin_id:
                raise DependencyResolutionError(
                    f"plugin cannot depend on itself: {plugin_dependency.plugin_id!r}",
                    source=definition.source,
                    plugin_id=plugin_id,
                    field=f"dependencies.plugins[{index}].id",
                )

        provided_names: set[str] = {service.name for service in definition.provided_services}

        for index, service_dependency in enumerate(definition.service_dependencies):
            if service_dependency.name in provided_names:
                raise DependencyResolutionError(
                    f"plugin cannot depend on its own service: {service_dependency.name!r}",
                    source=definition.source,
                    plugin_id=plugin_id,
                    field=f"dependencies.services[{index}].name",
                )


def _collect_dependencies(
    consumer: PluginDefinition,
    by_id: Mapping[str, PluginDefinition],
    services: Mapping[str, tuple[str, ProvidedService]],
    graph: dict[str, set[str]],
    bindings: list[Binding],
    unbound: list[UnboundDependency],
    optional: list[_Candidate],
) -> None:
    """把一个插件的声明分成强绑定、可选候选或未绑定记录。"""
    for index, plugin_dependency in enumerate(consumer.plugin_dependencies):
        provider = by_id.get(plugin_dependency.plugin_id)
        version = None if provider is None else provider.version
        _classify(
            consumer,
            kind="plugin",
            target=plugin_dependency.plugin_id,
            constraint=plugin_dependency.version,
            optional=plugin_dependency.optional,
            index=index,
            provider_id=None if provider is None else provider.plugin_id,
            provider_version=version,
            graph=graph,
            bindings=bindings,
            unbound=unbound,
            optional_candidates=optional,
        )

    for index, service_dependency in enumerate(consumer.service_dependencies):
        found = services.get(service_dependency.name)
        _classify(
            consumer,
            kind="service",
            target=service_dependency.name,
            constraint=service_dependency.version,
            optional=service_dependency.optional,
            index=index,
            provider_id=None if found is None else found[0],
            provider_version=None if found is None else found[1].version,
            graph=graph,
            bindings=bindings,
            unbound=unbound,
            optional_candidates=optional,
        )


def _classify(
    consumer: PluginDefinition,
    *,
    kind: str,
    target: str,
    constraint: str,
    optional: bool,
    index: int,
    provider_id: str | None,
    provider_version: str | None,
    graph: dict[str, set[str]],
    bindings: list[Binding],
    unbound: list[UnboundDependency],
    optional_candidates: list[_Candidate],
) -> None:
    """强依赖直接入图；可选依赖先记下，等强依赖图确认无环后再处理。"""
    field = _dependency_field(kind, index, provider_id is None)
    if provider_id is None or provider_version is None:
        _reject_or_record(consumer, kind, target, optional, field, "missing", unbound)
        return
    if not _version_matches(provider_version, constraint):
        _reject_or_record(
            consumer,
            kind,
            target,
            optional,
            field,
            "version_mismatch",
            unbound,
        )
        return

    candidate = _Candidate(consumer.plugin_id, kind, target, provider_id, optional)
    if optional:
        optional_candidates.append(candidate)
        return
    bindings.append(_binding(candidate))
    graph[provider_id].add(consumer.plugin_id)


def _reject_or_record(
    consumer: PluginDefinition,
    kind: str,
    target: str,
    optional: bool,
    field: str,
    reason: str,
    unbound: list[UnboundDependency],
) -> None:
    """强依赖失败则抛错；可选依赖记为未绑定，不中断整个计划。"""
    if not optional:
        raise DependencyResolutionError(
            f"{reason} dependency {target!r}",
            source=consumer.source,
            plugin_id=consumer.plugin_id,
            field=field,
        )
    unbound.append(UnboundDependency(consumer.plugin_id, kind, target, reason))


def _dependency_field(kind: str, index: int, missing: bool) -> str:
    """缺失指向标识字段，版本不匹配指向约束字段。"""
    group = "plugins" if kind == "plugin" else "services"
    if missing:
        leaf = "id" if kind == "plugin" else "name"
    else:
        leaf = "version"
    return f"dependencies.{group}[{index}].{leaf}"


def _apply_optional(
    graph: dict[str, set[str]],
    bindings: list[Binding],
    unbound: list[UnboundDependency],
    optional: list[_Candidate],
) -> None:
    """按固定键加入可选边；新边会成环时只屏蔽这一条。"""
    for candidate in sorted(optional, key=_candidate_key):
        provider_id = candidate.provider_id
        consumer_id = candidate.consumer_id
        if consumer_id in graph[provider_id]:
            bindings.append(_binding(candidate))
            continue
        path = _path_from(graph, consumer_id, provider_id)
        if path is None:
            graph[provider_id].add(consumer_id)
            bindings.append(_binding(candidate))
            continue
        unbound.append(
            UnboundDependency(
                consumer_id,
                candidate.kind,
                candidate.target,
                "cycle_masked",
                _canonicalize((provider_id,) + path),
            )
        )


def _require_acyclic(
    graph: Mapping[str, set[str]],
    by_id: Mapping[str, PluginDefinition],
) -> None:
    """强依赖图有环时给出闭合路径，不把所有剩余节点都当成环。"""
    order = _topological_order(graph)
    if len(order) == len(graph):
        return
    cycle = _find_cycle(graph, set(graph) - set(order))
    raise DependencyResolutionError(
        "dependency cycle " + " -> ".join(cycle),
        source=by_id[cycle[0]].source,
        plugin_id=cycle[0],
        path=cycle,
    )


def _topological_order(graph: Mapping[str, set[str]]) -> tuple[str, ...]:
    """Kahn 排序。每次从当前全部零入度节点里取最小 ID。"""
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


def _find_cycle(graph: Mapping[str, set[str]], nodes: set[str]) -> tuple[str, ...]:
    """迭代 DFS。只在给定节点子集里找一条从最小 ID 开始的闭合环。"""
    white, gray, black = 0, 1, 2
    color = dict.fromkeys(nodes, white)

    def neighbors(node: str) -> list[str]:
        return sorted(item for item in graph[node] if item in nodes)

    for start in sorted(nodes):
        if color[start] != white:
            continue
        path: list[str] = [start]
        color[start] = gray
        stack: list[tuple[str, list[str], int]] = [(start, neighbors(start), 0)]
        while stack:
            node, pending, cursor = stack[-1]
            if cursor >= len(pending):
                stack.pop()
                color[node] = black
                path.pop()
                continue
            stack[-1] = (node, pending, cursor + 1)
            nxt = pending[cursor]
            if color[nxt] == gray:
                index = path.index(nxt)
                return _canonicalize(tuple(path[index:] + [nxt]))
            if color[nxt] == white:
                color[nxt] = gray
                path.append(nxt)
                stack.append((nxt, neighbors(nxt), 0))

    raise DependencyResolutionError(
        "dependency cycle could not be traced",
        source=next(iter(sorted(nodes))),
        path=tuple(sorted(nodes)),
    )


def _path_from(
    graph: Mapping[str, set[str]],
    start: str,
    goal: str,
) -> tuple[str, ...] | None:
    """沿 Provider → Consumer 的现有边，查找从 start 到 goal 的稳定路径。"""
    queue: deque[tuple[str, tuple[str, ...]]] = deque([(start, (start,))])
    seen = {start}
    while queue:
        node, path = queue.popleft()
        for nxt in sorted(graph[node]):
            if nxt == goal:
                return path + (nxt,)
            if nxt not in seen:
                seen.add(nxt)
                queue.append((nxt, path + (nxt,)))
    return None


def _canonicalize(cycle: tuple[str, ...]) -> tuple[str, ...]:
    """把闭合路径旋转到最小 ID 开头，输入顺序变化时路径仍相同。"""
    body = list(cycle[:-1])
    index = body.index(min(body))
    rotated = body[index:] + body[:index]
    return tuple(rotated + [rotated[0]])


def _version_matches(version: str, constraint: str) -> bool:
    """按 PEP 440 比较。只有约束本身写了预发布时才接受预发布版本。"""
    specifier = SpecifierSet(constraint)
    return bool(specifier.contains(Version(version), prereleases=specifier.prereleases is True))


def _binding(candidate: _Candidate) -> Binding:
    return Binding(
        candidate.consumer_id,
        candidate.kind,
        candidate.target,
        candidate.provider_id,
    )


def _binding_key(binding: Binding) -> tuple[str, str, str, str]:
    return (binding.consumer_id, binding.kind, binding.target, binding.provider_id)


def _candidate_key(candidate: _Candidate) -> tuple[str, str, str, str]:
    return (candidate.consumer_id, candidate.kind, candidate.target, candidate.provider_id)


def _unbound_key(
    record: UnboundDependency,
) -> tuple[str, str, str, str, tuple[str, ...]]:
    return (record.consumer_id, record.kind, record.target, record.reason, record.cycle)
