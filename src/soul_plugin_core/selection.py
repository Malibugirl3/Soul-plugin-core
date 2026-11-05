"""对已经绑定的服务句柄进行显式、静态的名称选择。"""

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Generic, TypeVar

from soul_plugin_core.services import ServiceHandle

T = TypeVar("T")


class ServiceSelectionError(LookupError):
    """指定名称不可用或未指定默认项，不会静默选择其他服务。"""

    def __init__(self, name: str | None, available: tuple[str, ...]) -> None:
        self.name = name
        self.available = available
        message = (
            "no default service selected" if name is None else f"unknown service choice {name!r}"
        )
        super().__init__(message)


@dataclass(frozen=True, init=False)
class ServiceSelector(Generic[T]):
    """名称到已绑定句柄的只读快照；不负责动态注册或服务发现。"""

    _choices: Mapping[str, ServiceHandle[T]]
    available: tuple[str, ...]
    default: str | None

    def __init__(
        self, choices: Mapping[str, ServiceHandle[T]], *, default: str | None = None
    ) -> None:
        if not isinstance(choices, Mapping):
            raise TypeError("choices must be a mapping")
        snapshot = dict(choices)
        for name, handle in snapshot.items():
            self._check_name(name)
            if not isinstance(handle, ServiceHandle):
                raise TypeError("each choice must be a ServiceHandle")
        available = tuple(sorted(snapshot))
        if default is not None:
            self._check_name(default)
            if default not in snapshot:
                raise ServiceSelectionError(default, available)
        object.__setattr__(self, "_choices", MappingProxyType(snapshot))
        object.__setattr__(self, "available", available)
        object.__setattr__(self, "default", default)

    @staticmethod
    def _check_name(name: object) -> None:
        if not isinstance(name, str):
            raise TypeError("choice name must be a string")
        if not name or name != name.strip():
            raise ValueError("choice name must be non-empty without surrounding whitespace")

    def select(self, name: str | None = None) -> ServiceHandle[T]:
        """获取指定或显式配置的默认句柄；不存在时抛出 ServiceSelectionError。"""
        selected = self.default if name is None else name
        if selected is None:
            raise ServiceSelectionError(None, self.available)
        self._check_name(selected)
        try:
            return self._choices[selected]
        except KeyError as exc:
            raise ServiceSelectionError(selected, self.available) from exc
