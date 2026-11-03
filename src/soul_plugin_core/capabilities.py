"""宿主提供的能力授权快照；声明请求本身不代表已授权。"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from soul_plugin_core.contracts import PluginDefinition
from soul_plugin_core.errors import CapabilityDeniedError


@dataclass(frozen=True, init=False)
class CapabilityPolicy:
    """按实例标识保存允许的能力，名称使用精确匹配。"""

    _grants: Mapping[str, frozenset[str]]

    def __init__(self, grants: Mapping[str, Iterable[str]] | None = None) -> None:
        if grants is None:
            grants = {}
        if not isinstance(grants, Mapping):
            raise TypeError("capability grants must be a mapping")

        snapshot: dict[str, frozenset[str]] = {}
        for instance_id, capabilities in grants.items():
            _check_name(instance_id, "instance_id")
            if isinstance(capabilities, (str, bytes)) or not isinstance(capabilities, Iterable):
                raise TypeError("instance capability grants must be an iterable of strings")
            names: set[str] = set()
            for capability in capabilities:
                _check_name(capability, "capability")
                names.add(capability)
            snapshot[instance_id] = frozenset(names)
        object.__setattr__(self, "_grants", MappingProxyType(snapshot))

    @property
    def grants(self) -> Mapping[str, frozenset[str]]:
        """只读快照；修改传入的映射或集合不会改变它。"""
        return self._grants

    def grants_for(self, instance_id: str) -> frozenset[str]:
        return self._grants.get(instance_id, frozenset())

    def authorize(self, instance_id: str, definition: PluginDefinition) -> frozenset[str]:
        """审核一个实例，返回该实例声明且被授予的能力。"""
        requested = frozenset(definition.capability_requests)
        allowed = self.grants_for(instance_id)
        missing = requested - allowed
        if missing:
            raise CapabilityDeniedError(
                plugin_id=definition.plugin_id,
                instance_id=instance_id,
                source=definition.source,
                missing=sorted(missing),
            )
        return requested


def _check_name(value: object, label: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    if not value or value != value.strip():
        raise ValueError(f"{label} must be a non-empty string without surrounding whitespace")
