from collections.abc import Iterable
from dataclasses import dataclass


class ManifestValidationError(Exception):
    def __init__(
        self,
        message: str,
        *,
        source: str,
        field: str | None = None,
        plugin_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        self.source = source
        self.field = field
        self.plugin_id = plugin_id
        self.cause = cause
        parts = [message]
        if plugin_id is not None:
            parts.append(f"plugin_id={plugin_id}")
        if field is not None:
            parts.append(f"field={field}")
        parts.append(f"source={source}")
        super().__init__("; ".join(parts))


class DependencyResolutionError(Exception):
    def __init__(
        self,
        message: str,
        *,
        source: str,
        plugin_id: str | None = None,
        field: str | None = None,
        path: Iterable[str] = (),
        cause: BaseException | None = None,
    ) -> None:
        self.source = source
        self.plugin_id = plugin_id
        self.field = field
        self.path = tuple(path)
        self.cause = cause
        parts = [message]
        if plugin_id is not None:
            parts.append(f"plugin_id={plugin_id}")
        if field is not None:
            parts.append(f"field={field}")
        if self.path:
            parts.append("path=" + " -> ".join(self.path))
        parts.append(f"source={source}")
        super().__init__("; ".join(parts))


class InstanceResolutionError(Exception):
    """实例声明无法展开。发生在读取配置和导入入口之前。"""

    def __init__(
        self,
        message: str,
        *,
        plugin_id: str | None = None,
        instance_id: str | None = None,
        field: str | None = None,
        path: Iterable[str] = (),
        cause: BaseException | None = None,
    ) -> None:
        self.plugin_id = plugin_id
        self.instance_id = instance_id
        self.field = field
        self.source = "instance-declaration"
        self.path = tuple(path)
        self.cause = cause
        parts = [message]
        if plugin_id is not None:
            parts.append(f"plugin_id={plugin_id}")
        if instance_id is not None:
            parts.append(f"instance_id={instance_id}")
        if field is not None:
            parts.append(f"field={field}")
        if self.path:
            parts.append("path=" + " -> ".join(self.path))
        parts.append("source=instance-declaration")
        super().__init__("; ".join(parts))


class ConfigValidationError(Exception):
    """部署配置或 Schema 不合法。文本不包含配置值。"""

    def __init__(
        self,
        message: str,
        *,
        plugin_id: str | None,
        instance_id: str | None,
        phase: str,
        source: str,
        config_path: str,
        schema_path: str | None = None,
        json_path: str = "",
        cause: BaseException | None = None,
    ) -> None:
        self.plugin_id = plugin_id
        self.instance_id = instance_id
        self.phase = phase
        self.source = source
        self.config_path = config_path
        self.schema_path = schema_path
        self.json_path = json_path
        self.cause = cause
        parts = [message, f"phase={phase}"]
        if plugin_id is not None:
            parts.append(f"plugin_id={plugin_id}")
        if instance_id is not None:
            parts.append(f"instance_id={instance_id}")
        if json_path:
            parts.append(f"json_path={json_path}")
        parts.append(f"source={source}")
        super().__init__("; ".join(parts))


class PluginLoadError(Exception):
    """入口无法导入、构造或不符合异步生命周期。"""

    def __init__(
        self,
        message: str,
        *,
        plugin_id: str,
        instance_id: str,
        phase: str,
        source: str,
        cause: BaseException | None = None,
    ) -> None:
        self.plugin_id = plugin_id
        self.instance_id = instance_id
        self.phase = phase
        self.source = source
        self.cause = cause
        self.rollback_errors: tuple[BaseException, ...] = ()
        super().__init__(
            f"{message}; phase={phase}; plugin_id={plugin_id}; "
            f"instance_id={instance_id}; source={source}"
        )


class CapabilityDeniedError(Exception):
    """插件请求了当前实例未获授权的必需能力。"""

    def __init__(
        self,
        *,
        plugin_id: str,
        instance_id: str,
        source: str,
        missing: Iterable[str],
    ) -> None:
        self.plugin_id = plugin_id
        self.instance_id = instance_id
        self.phase = "capabilities"
        self.source = source
        self.missing = tuple(sorted(missing))
        self.cause: BaseException | None = None
        super().__init__(
            f"capability request denied; phase={self.phase}; plugin_id={plugin_id}; "
            f"instance_id={instance_id}; missing={self.missing!r}; source={source}"
        )


@dataclass(frozen=True)
class EventListenerFailure:
    """一次事件派发中某个监听器的失败。"""

    plugin_id: str
    instance_id: str
    cause: Exception


class EventDispatchError(Exception):
    """事件派发完成，但至少一个监听器发生普通异常。"""

    def __init__(
        self,
        *,
        event: str,
        plugin_id: str,
        instance_id: str,
        source: str,
        failures: Iterable[EventListenerFailure],
    ) -> None:
        self.event = event
        self.plugin_id = plugin_id
        self.instance_id = instance_id
        self.phase = "events"
        self.source = source
        self.failures = tuple(failures)
        self.errors = tuple(item.cause for item in self.failures)
        self.cause: Exception | None = self.errors[0] if self.errors else None
        super().__init__(
            f"event dispatch failed; event={event!r}; phase={self.phase}; "
            f"plugin_id={plugin_id}; instance_id={instance_id}; "
            f"failures={len(self.failures)}; source={source}"
        )


class PluginActivationError(Exception):
    """setup/start 失败，或在终态上再次激活。"""

    def __init__(
        self,
        message: str,
        *,
        plugin_id: str | None,
        instance_id: str | None,
        phase: str,
        source: str,
        cause: BaseException | None = None,
    ) -> None:
        self.plugin_id = plugin_id
        self.instance_id = instance_id
        self.phase = phase
        self.source = source
        self.cause = cause
        self.rollback_errors: tuple[BaseException, ...] = ()
        identity = ""
        if plugin_id is not None:
            identity += f"; plugin_id={plugin_id}"
        if instance_id is not None:
            identity += f"; instance_id={instance_id}"
        super().__init__(f"{message}; phase={phase}{identity}; source={source}")


class ServiceUnavailableError(Exception):
    """Handle 或 Provider generation 已不可调用。它不是激活错误。"""

    def __init__(
        self,
        message: str,
        *,
        name: str,
        provider_plugin_id: str,
        provider_instance_id: str,
        generation: int,
        cause: BaseException | None = None,
    ) -> None:
        self.name = name
        self.provider_plugin_id = provider_plugin_id
        self.provider_instance_id = provider_instance_id
        self.generation = generation
        self.cause = cause
        super().__init__(
            f"{message}; service={name}; provider_plugin_id={provider_plugin_id}; "
            f"provider_instance_id={provider_instance_id}; generation={generation}"
        )


class PluginCleanupError(Exception):
    """停止或清理失败。errors 保留每一项，不互相覆盖。"""

    def __init__(
        self,
        message: str,
        *,
        plugin_id: str | None,
        instance_id: str | None,
        phase: str,
        source: str,
        errors: Iterable[BaseException],
        cause: BaseException | None = None,
    ) -> None:
        self.plugin_id = plugin_id
        self.instance_id = instance_id
        self.phase = phase
        self.source = source
        self.errors = tuple(errors)
        self.cause = cause if cause is not None else (self.errors[0] if self.errors else None)
        super().__init__(f"{message}; phase={phase}; errors={len(self.errors)}; source={source}")
