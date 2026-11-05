"""基于 Manifest 的静态插件集合与可检查的组合计划。"""

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

from soul_plugin_core.capabilities import CapabilityPolicy
from soul_plugin_core.contracts import PluginDefinition
from soul_plugin_core.dependency import ActivationPlan, build_activation_plan
from soul_plugin_core.host import PluginHost
from soul_plugin_core.instances import (
    InstanceActivationPlan,
    InstanceDefinition,
    expand_instance_plan,
)
from soul_plugin_core.manifest import load_manifest


@dataclass(frozen=True, init=False)
class PluginBundle:
    """不可变的 Manifest 路径集合；不导入插件，也不声称拥有插件实例。"""

    manifests: tuple[Path, ...]

    def __init__(self, manifests: Iterable[str | Path]) -> None:
        if isinstance(manifests, (str, bytes, Path)):
            raise TypeError("manifests must be an iterable of paths")
        object.__setattr__(self, "manifests", tuple(Path(path).absolute() for path in manifests))

    @classmethod
    def from_manifests(cls, *manifests: str | Path) -> "PluginBundle":
        """从一个或多个 Manifest 路径创建集合。"""
        return cls(manifests)


@dataclass(frozen=True)
class CompositionPlan:
    """已解析的插件、实例计划与创建 Host 所需的部署输入。"""

    definitions: tuple[PluginDefinition, ...]
    config_path: Path
    instances: tuple[InstanceDefinition, ...] | None
    capability_policy: CapabilityPolicy | None
    plugin_plan: ActivationPlan = field(init=False)
    instance_plan: InstanceActivationPlan = field(init=False)

    def __post_init__(self) -> None:
        # Runtime callers can supply mutable lists despite tuple annotations.
        # Snapshot nested declarations before deriving either inspectable plan.
        definitions = tuple(
            PluginDefinition(
                plugin_id=item.plugin_id,
                version=item.version,
                api_version=item.api_version,
                entry_point=item.entry_point,
                plugin_dependencies=tuple(item.plugin_dependencies),
                service_dependencies=tuple(item.service_dependencies),
                provided_services=tuple(item.provided_services),
                capability_requests=tuple(item.capability_requests),
                config_schema=item.config_schema,
                source=item.source,
            )
            for item in self.definitions
        )
        declarations = (
            tuple(
                InstanceDefinition(item.instance_id, item.plugin_id, tuple(item.bindings))
                for item in self.instances
            )
            if self.instances is not None
            else None
        )
        if self.capability_policy is not None and not isinstance(
            self.capability_policy, CapabilityPolicy
        ):
            raise TypeError("capability_policy must be a CapabilityPolicy")
        policy = (
            CapabilityPolicy(self.capability_policy.grants)
            if self.capability_policy is not None
            else None
        )
        plugin_plan = build_activation_plan(definitions)
        instance_plan = expand_instance_plan(definitions, plugin_plan, declarations)
        object.__setattr__(self, "definitions", definitions)
        object.__setattr__(self, "config_path", Path(self.config_path).absolute())
        object.__setattr__(self, "instances", declarations)
        object.__setattr__(self, "capability_policy", policy)
        object.__setattr__(self, "plugin_plan", plugin_plan)
        object.__setattr__(self, "instance_plan", instance_plan)

    def create_host(self) -> PluginHost:
        """创建未激活的 Host；配置和授权由 activate 预检。"""
        return PluginHost(
            self.definitions,
            self.plugin_plan,
            config_path=self.config_path,
            instances=self.instances,
            capability_policy=self.capability_policy,
        )


def compose_plugins(
    *bundles: PluginBundle,
    config_path: str | Path,
    instances: Iterable[InstanceDefinition] | None = None,
    capability_policy: CapabilityPolicy | None = None,
) -> CompositionPlan:
    """合并声明并生成静态计划，不读取部署配置或导入插件入口。"""
    if capability_policy is not None and not isinstance(capability_policy, CapabilityPolicy):
        raise TypeError("capability_policy must be a CapabilityPolicy")
    if any(not isinstance(bundle, PluginBundle) for bundle in bundles):
        raise TypeError("each bundle must be a PluginBundle")
    definitions: tuple[PluginDefinition, ...] = tuple(
        load_manifest(path) for bundle in bundles for path in bundle.manifests
    )
    declarations = tuple(instances) if instances is not None else None
    return CompositionPlan(
        definitions,
        Path(config_path).absolute(),
        declarations,
        capability_policy,
    )
