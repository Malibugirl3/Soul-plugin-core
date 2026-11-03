from dataclasses import dataclass


@dataclass(frozen=True)
class PluginDependency:
    plugin_id: str
    version: str
    optional: bool


@dataclass(frozen=True)
class ServiceDependency:
    name: str
    version: str
    optional: bool


@dataclass(frozen=True)
class ProvidedService:
    name: str
    version: str


@dataclass(frozen=True)
class PluginDefinition:
    plugin_id: str
    version: str
    api_version: str
    entry_point: str
    plugin_dependencies: tuple[PluginDependency, ...]
    service_dependencies: tuple[ServiceDependency, ...]
    provided_services: tuple[ProvidedService, ...]
    capability_requests: tuple[str, ...]
    config_schema: str | None
    source: str
