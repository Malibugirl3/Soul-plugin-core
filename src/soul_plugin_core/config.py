"""部署配置与 JSON Schema 预验证。成功前不得导入插件入口。"""

import json
import sys
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError
from referencing import Registry
from referencing.exceptions import Unresolvable

from soul_plugin_core.contracts import PluginDefinition
from soul_plugin_core.errors import ConfigValidationError

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

_DRAFT_2020_12 = "https://json-schema.org/draft/2020-12/schema"
_SUPPORTED_SCHEMAS = {_DRAFT_2020_12, f"{_DRAFT_2020_12}#"}
_PHASE = "config"


def validate_deployment(
    instances: Mapping[str, PluginDefinition],
    config_path: Path,
) -> dict[str, Mapping[str, object]]:
    """按实例校验。键是 instance_id，Schema 来自对应插件。失败时还不能 import。"""
    source = str(config_path)
    document = _read_config(config_path)
    raw = _instance_tables(document, instances, source)
    frozen: dict[str, Mapping[str, object]] = {}
    for instance_id, definition in instances.items():
        config = raw.get(instance_id, {})
        if definition.config_schema is None:
            if config:
                raise ConfigValidationError(
                    "plugin without a schema only accepts empty configuration",
                    plugin_id=definition.plugin_id,
                    instance_id=instance_id,
                    phase=_PHASE,
                    source=source,
                    config_path=source,
                )
            frozen[instance_id] = _freeze_mapping({})
            continue
        schema_path = _locate_schema(definition, source, instance_id)
        schema = _load_schema(definition, schema_path, source, instance_id)
        _validate_instance(definition, instance_id, config, schema, schema_path, source)
        frozen_value = _freeze(config)
        if not isinstance(frozen_value, Mapping):
            raise ConfigValidationError(
                "instance configuration must be a table",
                plugin_id=definition.plugin_id,
                instance_id=instance_id,
                phase=_PHASE,
                source=source,
                config_path=source,
                schema_path=str(schema_path),
            )
        frozen[instance_id] = frozen_value
    return frozen


def _read_config(config_path: Path) -> dict[str, object]:
    source = str(config_path)
    try:
        text = config_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigValidationError(
            "cannot read deployment configuration",
            plugin_id=None,
            instance_id=None,
            phase=_PHASE,
            source=source,
            config_path=source,
            cause=exc,
        ) from exc
    except UnicodeDecodeError:
        # Decode errors retain the original input bytes. Do not expose them through
        # cause, context, or a formatted traceback.
        invalid_encoding = True
    else:
        invalid_encoding = False
    if invalid_encoding:
        raise ConfigValidationError(
            "deployment configuration is not valid UTF-8",
            plugin_id=None,
            instance_id=None,
            phase=_PHASE,
            source=source,
            config_path=source,
        )
    try:
        document = tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        # TOML parser exceptions may retain the complete input document.
        invalid_toml = True
    else:
        invalid_toml = False
    if invalid_toml:
        raise ConfigValidationError(
            "deployment configuration is not valid TOML",
            plugin_id=None,
            instance_id=None,
            phase=_PHASE,
            source=source,
            config_path=source,
        )
    if not isinstance(document, dict):
        raise ConfigValidationError(
            "deployment configuration must be a table",
            plugin_id=None,
            instance_id=None,
            phase=_PHASE,
            source=source,
            config_path=source,
        )
    return document


def _instance_tables(
    document: Mapping[str, object],
    instances: Mapping[str, PluginDefinition],
    source: str,
) -> dict[str, dict[str, object]]:
    unknown_root = sorted(set(document) - {"plugins"})
    if unknown_root:
        raise ConfigValidationError(
            f"unknown deployment field {unknown_root[0]!r}",
            plugin_id=None,
            instance_id=None,
            phase=_PHASE,
            source=source,
            config_path=source,
            json_path=unknown_root[0],
        )
    plugins = document.get("plugins", {})
    if not isinstance(plugins, dict):
        raise ConfigValidationError(
            "plugins must be a table",
            plugin_id=None,
            instance_id=None,
            phase=_PHASE,
            source=source,
            config_path=source,
            json_path="plugins",
        )
    known = set(instances)
    tables: dict[str, dict[str, object]] = {}
    for instance_id in sorted(plugins):
        if instance_id not in known:
            raise ConfigValidationError(
                f"unknown instance configuration {instance_id!r}",
                plugin_id=None,
                instance_id=instance_id,
                phase=_PHASE,
                source=source,
                config_path=source,
                json_path=f"plugins.{instance_id}",
            )
        table = plugins[instance_id]
        if not isinstance(table, dict):
            raise ConfigValidationError(
                "instance configuration must be a table",
                plugin_id=instances[instance_id].plugin_id,
                instance_id=instance_id,
                phase=_PHASE,
                source=source,
                config_path=source,
                json_path=f"plugins.{instance_id}",
            )
        tables[instance_id] = table
    return tables


def _locate_schema(
    definition: PluginDefinition,
    config_source: str,
    instance_id: str,
) -> Path:
    """打开文件前确认真实路径仍在 Manifest 目录内。"""
    schema_name = definition.config_schema
    if schema_name is None:
        raise ConfigValidationError(
            "schema path is missing",
            plugin_id=definition.plugin_id,
            instance_id=instance_id,
            phase=_PHASE,
            source=config_source,
            config_path=config_source,
        )
    root = Path(definition.source).parent.resolve(strict=True)
    candidate = Path(schema_name)
    try:
        resolved = (root / candidate).resolve(strict=True)
    except OSError as exc:
        raise ConfigValidationError(
            "schema file is not available inside the plugin directory",
            plugin_id=definition.plugin_id,
            instance_id=instance_id,
            phase=_PHASE,
            source=definition.source,
            config_path=config_source,
            schema_path=str(root / candidate),
            cause=exc,
        ) from exc
    if not resolved.is_relative_to(root):
        raise ConfigValidationError(
            "schema path escapes the plugin directory",
            plugin_id=definition.plugin_id,
            instance_id=instance_id,
            phase=_PHASE,
            source=definition.source,
            config_path=config_source,
            schema_path=str(resolved),
        )
    return resolved


def _load_schema(
    definition: PluginDefinition,
    schema_path: Path,
    config_source: str,
    instance_id: str,
) -> dict[str, object]:
    try:
        text = schema_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigValidationError(
            "cannot read schema",
            plugin_id=definition.plugin_id,
            instance_id=instance_id,
            phase=_PHASE,
            source=definition.source,
            config_path=config_source,
            schema_path=str(schema_path),
            cause=exc,
        ) from exc
    except UnicodeDecodeError as exc:
        raise ConfigValidationError(
            "schema is not valid UTF-8",
            plugin_id=definition.plugin_id,
            instance_id=instance_id,
            phase=_PHASE,
            source=definition.source,
            config_path=config_source,
            schema_path=str(schema_path),
            cause=exc,
        ) from exc
    try:
        schema = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            "schema is not valid JSON",
            plugin_id=definition.plugin_id,
            instance_id=instance_id,
            phase=_PHASE,
            source=definition.source,
            config_path=config_source,
            schema_path=str(schema_path),
            cause=exc,
        ) from exc
    if not isinstance(schema, dict):
        raise ConfigValidationError(
            "schema must be a JSON object",
            plugin_id=definition.plugin_id,
            instance_id=instance_id,
            phase=_PHASE,
            source=definition.source,
            config_path=config_source,
            schema_path=str(schema_path),
        )
    declared = schema.get("$schema")
    if declared is not None and (
        not isinstance(declared, str) or declared not in _SUPPORTED_SCHEMAS
    ):
        raise ConfigValidationError(
            "schema draft is not supported",
            plugin_id=definition.plugin_id,
            instance_id=instance_id,
            phase=_PHASE,
            source=definition.source,
            config_path=config_source,
            schema_path=str(schema_path),
            json_path="$schema",
        )
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise ConfigValidationError(
            "schema document is invalid",
            plugin_id=definition.plugin_id,
            instance_id=instance_id,
            phase=_PHASE,
            source=definition.source,
            config_path=config_source,
            schema_path=str(schema_path),
            cause=exc,
        ) from exc
    return schema


def _validate_instance(
    definition: PluginDefinition,
    instance_id: str,
    config: Mapping[str, object],
    schema: Mapping[str, object],
    schema_path: Path,
    config_source: str,
) -> None:
    # An empty registry permits same-document references, but never retrieves
    # external documents implicitly (including HTTP and filesystem URLs).
    error_message: str | None = None
    pointer = ""
    try:
        Draft202012Validator(schema, registry=Registry()).validate(config)
    except ValidationError as exc:
        pointer = "/" + "/".join(str(part) for part in exc.absolute_path)
        error_message = "configuration does not match schema"
    except Unresolvable:
        error_message = "schema reference cannot be resolved"
    if error_message is not None:
        # Raise after the except block: ValidationError retains the entire input
        # instance, even if an exception chain is suppressed with ``from None``.
        raise ConfigValidationError(
            error_message,
            plugin_id=definition.plugin_id,
            instance_id=instance_id,
            phase=_PHASE,
            source=config_source,
            config_path=config_source,
            schema_path=str(schema_path),
            json_path=pointer,
        )


def _freeze(value: object) -> object:
    """递归冻结。list 变 tuple，set 变 frozenset，映射变只读视图。"""
    if isinstance(value, dict):
        return _freeze_mapping(value)
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze(item) for item in value)
    return value


def _freeze_mapping(value: Mapping[str, object]) -> Mapping[str, object]:
    return MappingProxyType({key: _freeze(item) for key, item in value.items()})
