"""Configuration failures do not expose values or resolve external schemas."""

import asyncio
import json
import socket
import traceback

import pytest

from soul_plugin_core.config import validate_deployment
from soul_plugin_core.contracts import PluginDefinition
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import ConfigValidationError
from soul_plugin_core.host import PluginHost


@pytest.fixture
def deployment(tmp_path):
    definition = PluginDefinition(
        plugin_id="worker",
        version="1",
        api_version="1",
        entry_point="config_security_fixture:Plugin",
        plugin_dependencies=(),
        service_dependencies=(),
        provided_services=(),
        capability_requests=(),
        config_schema="schema.json",
        source=str(tmp_path / "plugin.toml"),
    )
    (tmp_path / "schema.json").write_text("{}", encoding="utf-8")
    config = tmp_path / "config.toml"
    config.write_text('[plugins.worker]\nsecret = "SUPER_SECRET_TOKEN"\n', encoding="utf-8")
    return definition, config, tmp_path / "schema.json"


def test_invalid_value_is_absent_from_exception_chain_and_traceback(deployment):
    definition, config, schema = deployment
    schema.write_text(json.dumps({"properties": {"secret": {"type": "integer"}}}))
    with pytest.raises(ConfigValidationError) as caught:
        validate_deployment({"worker": definition}, config)
    error = caught.value
    assert error.json_path == "/secret"
    assert error.cause is None
    assert error.__cause__ is None
    assert error.__context__ is None
    assert "SUPER_SECRET_TOKEN" not in "".join(traceback.format_exception(error))


@pytest.mark.parametrize(
    "raw", [b'[plugins.worker]\nsecret = "SUPER_SECRET_TOKEN', b"\xffSUPER_SECRET_TOKEN"]
)
def test_invalid_config_input_is_not_retained_in_errors(deployment, raw):
    definition, config, _ = deployment
    config.write_bytes(raw)
    with pytest.raises(ConfigValidationError) as caught:
        validate_deployment({"worker": definition}, config)
    error = caught.value
    assert error.cause is None
    assert error.__cause__ is None
    assert error.__context__ is None
    assert "SUPER_SECRET_TOKEN" not in "".join(traceback.format_exception(error))


def test_same_document_reference_still_works(deployment):
    definition, config, schema = deployment
    schema.write_text(
        json.dumps(
            {"$defs": {"s": {"type": "string"}}, "properties": {"secret": {"$ref": "#/$defs/s"}}}
        )
    )
    assert (
        validate_deployment({"worker": definition}, config)["worker"]["secret"]
        == "SUPER_SECRET_TOKEN"
    )


@pytest.mark.parametrize(
    "reference",
    [
        "http://127.0.0.1:12345/schema",
        "file:///not-allowed/schema.json",
        "other.json",
        "#/$defs/missing",
    ],
)
@pytest.mark.parametrize("keyword", ["$ref", "$dynamicRef"])
def test_unresolvable_references_are_errors_without_network_or_plugin_import(
    deployment, monkeypatch, tmp_path, reference, keyword
):
    definition, config, schema = deployment
    schema.write_text(json.dumps({keyword: reference}))
    monkeypatch.syspath_prepend(str(tmp_path))
    (tmp_path / "config_security_fixture.py").write_text(
        "raise AssertionError('plugin code was imported')\n", encoding="utf-8"
    )

    def unexpected_connect(*args, **kwargs):
        raise AssertionError("validation attempted a network connection")

    host = PluginHost((definition,), build_activation_plan((definition,)), config_path=config)

    async def activate_without_network():
        # Windows uses a socket pair to create an event loop; intercept connections
        # only after that loop exists and restore the method before it closes.
        with monkeypatch.context() as blocked:
            blocked.setattr(socket.socket, "connect", unexpected_connect)
            await host.activate()

    with pytest.raises(ConfigValidationError) as caught:
        asyncio.run(activate_without_network())
    assert caught.value.cause is None
    assert caught.value.__context__ is None
    assert "SUPER_SECRET_TOKEN" not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize("declared", [[], {}, 1, True])
def test_invalid_schema_draft_is_a_config_error(deployment, declared):
    definition, config, schema = deployment
    schema.write_text(json.dumps({"$schema": declared}))
    with pytest.raises(ConfigValidationError) as caught:
        validate_deployment({"worker": definition}, config)
    assert caught.value.json_path == "$schema"
