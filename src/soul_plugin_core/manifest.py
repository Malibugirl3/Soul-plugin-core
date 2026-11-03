import re
import sys
from pathlib import Path

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from soul_plugin_core.contracts import (
    PluginDefinition,
    PluginDependency,
    ProvidedService,
    ServiceDependency,
)
from soul_plugin_core.errors import ManifestValidationError

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

_ROOT_KEYS = {"manifest_version", "plugin", "dependencies", "provides", "capabilities", "config"}
_ENTRY_POINT = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*$"
)


def load_manifest(path):
    """读取并验证 TOML Manifest；只生成定义，不导入插件入口。"""
    path = Path(path)
    source = str(path)
    data = _read_toml(path)
    plugin_id = _peek_plugin_id(data.get("plugin"))
    _reject_unknown(data, _ROOT_KEYS, source, "", plugin_id)
    _require_manifest_version(data, source, plugin_id)
    plugin_id, version, api_version, entry_point = _parse_plugin(data, source)
    plugin_dependencies, service_dependencies = _parse_dependencies(data, source, plugin_id)
    return PluginDefinition(
        plugin_id=plugin_id,
        version=version,
        api_version=api_version,
        entry_point=entry_point,
        plugin_dependencies=plugin_dependencies,
        service_dependencies=service_dependencies,
        provided_services=_parse_provides(data, source, plugin_id),
        capability_requests=_parse_capabilities(data, source, plugin_id),
        config_schema=_parse_schema(data, source, plugin_id),
        source=source,
    )


def _read_toml(path):
    source = str(path)
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ManifestValidationError("cannot read manifest", source=source, cause=exc) from exc
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ManifestValidationError(
            "manifest is not valid UTF-8", source=source, cause=exc
        ) from exc
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise ManifestValidationError(
            "manifest is not valid TOML", source=source, cause=exc
        ) from exc
    if not isinstance(data, dict):
        raise ManifestValidationError("manifest root must be a table", source=source)
    return data


def _parse_plugin(data, source):
    plugin = data.get("plugin")
    if not isinstance(plugin, dict):
        raise ManifestValidationError("plugin table is required", source=source, field="plugin")
    plugin_id = _peek_plugin_id(plugin)
    _reject_unknown(
        plugin, {"id", "version", "api_version", "entry_point"}, source, "plugin", plugin_id
    )
    plugin_id = _require_text(plugin, "id", source, "plugin.id", plugin_id)
    version = _require_version(plugin, "version", source, "plugin.version", plugin_id)
    api_version = _require_text(plugin, "api_version", source, "plugin.api_version", plugin_id)
    if api_version != "1":
        raise ManifestValidationError(
            "unsupported api_version",
            source=source,
            field="plugin.api_version",
            plugin_id=plugin_id,
        )
    entry_point = _require_text(plugin, "entry_point", source, "plugin.entry_point", plugin_id)
    if _ENTRY_POINT.fullmatch(entry_point) is None:
        raise ManifestValidationError(
            "invalid entry_point",
            source=source,
            field="plugin.entry_point",
            plugin_id=plugin_id,
        )
    return plugin_id, version, api_version, entry_point


def _parse_dependencies(data, source, plugin_id):
    raw = _optional_table(data, "dependencies", source, plugin_id)
    if raw is None:
        return (), ()
    _reject_unknown(raw, {"plugins", "services"}, source, "dependencies", plugin_id)
    return (
        _parse_plugin_dependencies(raw.get("plugins", []), source, plugin_id),
        _parse_service_dependencies(raw.get("services", []), source, plugin_id),
    )


def _parse_plugin_dependencies(items, source, plugin_id):
    items = _require_array(items, source, "dependencies.plugins", plugin_id)
    seen = set()
    result = []
    for index, item in enumerate(items):
        field = f"dependencies.plugins[{index}]"
        _require_table(item, source, field, plugin_id)
        _reject_unknown(item, {"id", "version", "optional"}, source, field, plugin_id)
        dependency_id = _require_text(item, "id", source, f"{field}.id", plugin_id)
        if dependency_id in seen:
            raise ManifestValidationError(
                "duplicate plugin dependency",
                source=source,
                field=f"{field}.id",
                plugin_id=plugin_id,
            )
        seen.add(dependency_id)
        result.append(
            PluginDependency(
                plugin_id=dependency_id,
                version=_require_specifier(item, "version", source, f"{field}.version", plugin_id),
                optional=_require_bool(item, "optional", source, f"{field}.optional", plugin_id),
            )
        )
    return tuple(result)


def _parse_service_dependencies(items, source, plugin_id):
    items = _require_array(items, source, "dependencies.services", plugin_id)
    seen = set()
    result = []
    for index, item in enumerate(items):
        field = f"dependencies.services[{index}]"
        _require_table(item, source, field, plugin_id)
        _reject_unknown(item, {"name", "version", "optional"}, source, field, plugin_id)
        name = _require_text(item, "name", source, f"{field}.name", plugin_id)
        if name in seen:
            raise ManifestValidationError(
                "duplicate service dependency",
                source=source,
                field=f"{field}.name",
                plugin_id=plugin_id,
            )
        seen.add(name)
        result.append(
            ServiceDependency(
                name=name,
                version=_require_specifier(item, "version", source, f"{field}.version", plugin_id),
                optional=_require_bool(item, "optional", source, f"{field}.optional", plugin_id),
            )
        )
    return tuple(result)


def _parse_provides(data, source, plugin_id):
    raw = _optional_table(data, "provides", source, plugin_id)
    if raw is None:
        return ()
    _reject_unknown(raw, {"services"}, source, "provides", plugin_id)
    items = _require_array(raw.get("services", []), source, "provides.services", plugin_id)
    seen = set()
    result = []
    for index, item in enumerate(items):
        field = f"provides.services[{index}]"
        _require_table(item, source, field, plugin_id)
        _reject_unknown(item, {"name", "version"}, source, field, plugin_id)
        name = _require_text(item, "name", source, f"{field}.name", plugin_id)
        if name in seen:
            raise ManifestValidationError(
                "duplicate provided service",
                source=source,
                field=f"{field}.name",
                plugin_id=plugin_id,
            )
        seen.add(name)
        result.append(
            ProvidedService(
                name=name,
                version=_require_version(item, "version", source, f"{field}.version", plugin_id),
            )
        )
    return tuple(result)


def _parse_capabilities(data, source, plugin_id):
    raw = _optional_table(data, "capabilities", source, plugin_id)
    if raw is None:
        return ()
    _reject_unknown(raw, {"requests"}, source, "capabilities", plugin_id)
    items = _require_array(raw.get("requests", []), source, "capabilities.requests", plugin_id)
    seen = set()
    result = []
    for index, item in enumerate(items):
        field = f"capabilities.requests[{index}]"
        if not _is_plain_text(item):
            raise ManifestValidationError(
                "must be a non-empty string",
                source=source,
                field=field,
                plugin_id=plugin_id,
            )
        if item in seen:
            raise ManifestValidationError(
                "duplicate capability request",
                source=source,
                field=field,
                plugin_id=plugin_id,
            )
        seen.add(item)
        result.append(item)
    return tuple(result)


def _parse_schema(data, source, plugin_id):
    raw = _optional_table(data, "config", source, plugin_id)
    if raw is None:
        return None
    _reject_unknown(raw, {"schema"}, source, "config", plugin_id)
    if "schema" not in raw:
        return None
    value = raw["schema"]
    if not _is_plain_text(value):
        raise ManifestValidationError(
            "must be a non-empty string",
            source=source,
            field="config.schema",
            plugin_id=plugin_id,
        )
    if "\x00" in value:
        raise ManifestValidationError(
            "schema path contains NUL",
            source=source,
            field="config.schema",
            plugin_id=plugin_id,
        )
    parts = value.split("/")
    if (
        "\\" in value
        or ":" in value
        or value.startswith("/")
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise ManifestValidationError(
            "schema path must stay inside the plugin directory",
            source=source,
            field="config.schema",
            plugin_id=plugin_id,
        )
    return value


def _require_manifest_version(data, source, plugin_id):
    if "manifest_version" not in data:
        raise ManifestValidationError(
            "missing field",
            source=source,
            field="manifest_version",
            plugin_id=plugin_id,
        )
    value = data["manifest_version"]
    if type(value) is not int or value != 1:
        raise ManifestValidationError(
            "manifest_version must be integer 1",
            source=source,
            field="manifest_version",
            plugin_id=plugin_id,
        )


def _optional_table(data, key, source, plugin_id):
    if key not in data:
        return None
    return _require_table(data[key], source, key, plugin_id)


def _require_table(value, source, field, plugin_id):
    if not isinstance(value, dict):
        raise ManifestValidationError(
            "must be a table", source=source, field=field, plugin_id=plugin_id
        )
    return value


def _require_array(value, source, field, plugin_id):
    if not isinstance(value, list):
        raise ManifestValidationError(
            "must be an array", source=source, field=field, plugin_id=plugin_id
        )
    return value


def _require_text(table, key, source, field, plugin_id):
    if key not in table:
        raise ManifestValidationError(
            "missing field", source=source, field=field, plugin_id=plugin_id
        )
    value = table[key]
    if not _is_plain_text(value):
        raise ManifestValidationError(
            "must be a non-empty string",
            source=source,
            field=field,
            plugin_id=plugin_id,
        )
    return value


def _require_version(table, key, source, field, plugin_id):
    text = _require_text(table, key, source, field, plugin_id)
    try:
        Version(text)
    except InvalidVersion as exc:
        raise ManifestValidationError(
            "invalid version",
            source=source,
            field=field,
            plugin_id=plugin_id,
            cause=exc,
        ) from exc
    return text


def _require_specifier(table, key, source, field, plugin_id):
    text = _require_text(table, key, source, field, plugin_id)
    try:
        specifier = SpecifierSet(text)
    except InvalidSpecifier as exc:
        raise ManifestValidationError(
            "invalid version constraint",
            source=source,
            field=field,
            plugin_id=plugin_id,
            cause=exc,
        ) from exc
    if not specifier:
        raise ManifestValidationError(
            "empty version constraint",
            source=source,
            field=field,
            plugin_id=plugin_id,
        )
    return text


def _require_bool(table, key, source, field, plugin_id):
    if key not in table:
        return False
    value = table[key]
    if type(value) is not bool:
        raise ManifestValidationError(
            "must be a boolean", source=source, field=field, plugin_id=plugin_id
        )
    return value


def _reject_unknown(mapping, allowed, source, prefix, plugin_id):
    for key in mapping:
        if key not in allowed:
            field = f"{prefix}.{key}" if prefix else key
            raise ManifestValidationError(
                "unknown field", source=source, field=field, plugin_id=plugin_id
            )


def _peek_plugin_id(plugin):
    if not isinstance(plugin, dict):
        return None
    value = plugin.get("id")
    if _is_plain_text(value):
        return value
    return None


def _is_plain_text(value):
    return isinstance(value, str) and value == value.strip() and bool(value)
