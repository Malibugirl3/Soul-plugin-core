import sys
from pathlib import Path

import pytest

from soul_plugin_core.errors import ManifestValidationError
from soul_plugin_core.manifest import load_manifest

FIXTURE = Path(__file__).parent / "fixtures" / "no_import_plugin"
SENTINEL = FIXTURE / "imported.txt"

MINIMAL = """\
manifest_version = 1

[plugin]
id = "demo.greeter"
version = "0.1.0"
api_version = "1"
entry_point = "sentinel_plugin:GreeterPlugin"
"""

FULL = """\
manifest_version = 1

[plugin]
id = "demo.greeter"
version = "1.0.0+local"
api_version = "1"
entry_point = "sentinel_plugin:GreeterPlugin"

[dependencies]
plugins = [
  { id = "demo.base", version = ">=1.0", optional = true }
]
services = [
  { name = "demo.clock", version = ">=1.0,<2.0" }
]

[provides]
services = [
  { name = "demo.greeting", version = "1.0.0" }
]

[capabilities]
requests = ["logging.write"]

[config]
schema = "config.schema.json"
"""


def write_manifest(directory, text):
    path = directory / "plugin.toml"
    path.write_text(text, encoding="utf-8")
    return path


def parse(directory, text):
    sys.path.insert(0, str(FIXTURE))
    SENTINEL.unlink(missing_ok=True)
    try:
        return load_manifest(write_manifest(directory, text))
    finally:
        assert "sentinel_plugin" not in sys.modules
        assert not SENTINEL.exists()
        sys.path.remove(str(FIXTURE))


def test_minimal_manifest(tmp_path):
    definition = parse(tmp_path, MINIMAL)
    assert definition.plugin_id == "demo.greeter"
    assert definition.plugin_dependencies == ()
    assert definition.service_dependencies == ()
    assert definition.provided_services == ()
    assert definition.capability_requests == ()
    assert definition.config_schema is None


def test_full_manifest(tmp_path):
    definition = parse(tmp_path, FULL)
    assert definition.plugin_dependencies[0].plugin_id == "demo.base"
    assert definition.plugin_dependencies[0].optional is True
    assert definition.service_dependencies[0].version == ">=1.0,<2.0"
    assert definition.provided_services[0].name == "demo.greeting"
    assert definition.capability_requests == ("logging.write",)
    assert definition.config_schema == "config.schema.json"


def test_definition_is_immutable(tmp_path):
    definition = parse(tmp_path, MINIMAL)
    with pytest.raises(AttributeError):
        definition.plugin_id = "other"
    assert isinstance(definition.capability_requests, tuple)


@pytest.mark.parametrize(
    ("text", "field"),
    [
        ("manifest_version = 1\n", "plugin"),
        # (MINIMAL + "extra = 1\n", "extra"),
        (MINIMAL.replace("manifest_version = 1\n", "manifest_version = 1\nextra = 1\n"), "extra"),
        (MINIMAL.replace('api_version = "1"\n', 'api_version = "1"\nextra = 1\n'), "plugin.extra"),
        (MINIMAL.replace('version = "0.1.0"', "version = true"), "plugin.version"),
        (MINIMAL.replace('version = "0.1.0"', "version = 1"), "plugin.version"),
        (MINIMAL.replace('version = "0.1.0"', 'version = "not a version"'), "plugin.version"),
        (MINIMAL.replace("sentinel_plugin:GreeterPlugin", "not-an-entry"), "plugin.entry_point"),
    ],
)
def test_invalid_manifest(tmp_path, text, field):
    with pytest.raises(ManifestValidationError) as caught:
        parse(tmp_path, text)
    assert caught.value.field == field


def test_invalid_dependency_constraint(tmp_path):
    text = MINIMAL + '\n[dependencies]\nservices = [{ name = "demo.clock", version = ">>1" }]\n'
    with pytest.raises(ManifestValidationError) as caught:
        parse(tmp_path, text)
    assert caught.value.field == "dependencies.services[0].version"
    assert caught.value.plugin_id == "demo.greeter"
    assert caught.value.cause is not None


def test_duplicate_provided_service(tmp_path):
    text = (
        MINIMAL
        + """
[provides]
services = [
  { name = "demo.greeting", version = "1.0.0" },
  { name = "demo.greeting", version = "1.0.1" }
]
"""
    )
    with pytest.raises(ManifestValidationError) as caught:
        parse(tmp_path, text)
    assert caught.value.field == "provides.services[1].name"


@pytest.mark.parametrize("schema", ["../config.schema.json", "/tmp/config.schema.json"])
def test_schema_path_must_stay_inside_plugin(tmp_path, schema):
    text = MINIMAL + f'\n[config]\nschema = "{schema}"\n'
    with pytest.raises(ManifestValidationError) as caught:
        parse(tmp_path, text)
    assert caught.value.field == "config.schema"


def test_missing_file(tmp_path):
    with pytest.raises(ManifestValidationError) as caught:
        load_manifest(tmp_path / "missing.toml")
    assert caught.value.cause is not None


def test_invalid_toml(tmp_path):
    path = tmp_path / "plugin.toml"
    path.write_text("manifest_version = [\n", encoding="utf-8")
    with pytest.raises(ManifestValidationError) as caught:
        load_manifest(path)
    assert caught.value.cause is not None


def test_blank_identifier_is_rejected(tmp_path):
    text = MINIMAL.replace('id = "demo.greeter"', 'id = "   "')
    with pytest.raises(ManifestValidationError) as caught:
        parse(tmp_path, text)
    assert caught.value.field == "plugin.id"


def test_blank_capability_is_rejected(tmp_path):
    text = MINIMAL + '\n[capabilities]\nrequests = ["   "]\n'
    with pytest.raises(ManifestValidationError) as caught:
        parse(tmp_path, text)
    assert caught.value.field == "capabilities.requests[0]"
    assert caught.value.plugin_id == "demo.greeter"


def test_schema_nul_is_rejected(tmp_path):
    text = MINIMAL + '\n[config]\nschema = "bad\\u0000.json"\n'
    with pytest.raises(ManifestValidationError) as caught:
        parse(tmp_path, text)
    assert caught.value.field == "config.schema"
    assert caught.value.plugin_id == "demo.greeter"


def test_known_plugin_id_is_included(tmp_path):
    text = MINIMAL.replace(
        'entry_point = "sentinel_plugin:GreeterPlugin"\n',
        'entry_point = "sentinel_plugin:GreeterPlugin"\ntypo = 1\n',
    )
    with pytest.raises(ManifestValidationError) as caught:
        parse(tmp_path, text)
    assert caught.value.field == "plugin.typo"
    assert caught.value.plugin_id == "demo.greeter"
    assert "plugin.typo" in str(caught.value)
    assert "demo.greeter" in str(caught.value)


@pytest.mark.parametrize(
    ("text", "field"),
    [
        (
            """\
[plugin]
id = "demo.greeter"
version = "0.1.0"
api_version = "1"
entry_point = "sentinel_plugin:GreeterPlugin"
""",
            "manifest_version",
        ),
        (MINIMAL.replace("manifest_version = 1", "manifest_version = true"), "manifest_version"),
        (MINIMAL.replace("manifest_version = 1", "manifest_version = 1.0"), "manifest_version"),
        (MINIMAL.replace('api_version = "1"', 'api_version = "2"'), "plugin.api_version"),
        (
            MINIMAL
            + '\n[dependencies]\nservices = [{ name = "demo.clock", version = ">=1", optional = "yes" }]\n',
            "dependencies.services[0].optional",
        ),
        (
            MINIMAL + '\n[dependencies]\nservices = [{ name = "demo.clock", version = "" }]\n',
            "dependencies.services[0].version",
        ),
        (
            MINIMAL
            + '\n[dependencies]\nplugins = [{ id = "demo.base", version = ">=1" }, { id = "demo.base", version = ">=2" }]\n',
            "dependencies.plugins[1].id",
        ),
        (
            MINIMAL + '\n[capabilities]\nrequests = ["logging.write", "logging.write"]\n',
            "capabilities.requests[1]",
        ),
        (MINIMAL + "\n[dependencies]\nextra = 1\n", "dependencies.extra"),
    ],
)
def test_more_invalid_manifests(tmp_path, text, field):
    with pytest.raises(ManifestValidationError) as caught:
        parse(tmp_path, text)
    assert caught.value.field == field
    assert caught.value.plugin_id == "demo.greeter"


def test_bad_utf8(tmp_path):
    path = tmp_path / "plugin.toml"
    path.write_bytes(b"\xff")
    with pytest.raises(ManifestValidationError) as caught:
        load_manifest(path)
    assert caught.value.cause is not None
    assert str(path) in str(caught.value)


def test_wrong_table_type(tmp_path):
    text = MINIMAL + "\n[dependencies]\nplugins = 1\n"
    with pytest.raises(ManifestValidationError) as caught:
        parse(tmp_path, text)
    assert caught.value.field == "dependencies.plugins"
    assert caught.value.plugin_id == "demo.greeter"
