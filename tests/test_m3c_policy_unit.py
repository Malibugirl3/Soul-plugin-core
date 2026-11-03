"""Capability policy has a stable immutable snapshot independent of Host."""

from dataclasses import FrozenInstanceError

import pytest

from soul_plugin_core.capabilities import CapabilityPolicy
from soul_plugin_core.contracts import PluginDefinition
from soul_plugin_core.errors import CapabilityDeniedError


def definition(requests: tuple[str, ...]) -> PluginDefinition:
    return PluginDefinition(
        plugin_id="worker",
        version="1",
        api_version="1",
        entry_point="not_imported:Plugin",
        plugin_dependencies=(),
        service_dependencies=(),
        provided_services=(),
        capability_requests=requests,
        config_schema=None,
        source="worker/plugin.toml",
    )


def test_policy_is_an_immutable_snapshot() -> None:
    source = {"worker-us": {"logging.write"}}
    policy = CapabilityPolicy(source)
    source["worker-us"].clear()
    source["new-instance"] = {"network.connect"}
    assert policy.grants_for("worker-us") == frozenset({"logging.write"})
    assert policy.grants_for("new-instance") == frozenset()
    with pytest.raises(TypeError):
        policy.grants["worker-us"] = frozenset()  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        policy._grants = {}  # type: ignore[misc]


def test_policy_returns_only_requested_capabilities() -> None:
    policy = CapabilityPolicy({"worker-us": iter(("logging.write", "network.connect"))})
    granted = policy.authorize("worker-us", definition(("logging.write",)))
    assert granted == frozenset({"logging.write"})


def test_denial_has_stable_instance_identity_and_missing_names() -> None:
    policy = CapabilityPolicy({"worker-us": {"logging.write"}})
    with pytest.raises(CapabilityDeniedError) as caught:
        policy.authorize("worker-eu", definition(("network.connect", "logging.write")))
    assert caught.value.plugin_id == "worker"
    assert caught.value.instance_id == "worker-eu"
    assert caught.value.source == "worker/plugin.toml"
    assert caught.value.phase == "capabilities"
    assert caught.value.missing == ("logging.write", "network.connect")
    assert caught.value.cause is None
