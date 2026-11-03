"""M3c: capability requests are audited before any plugin import."""

import asyncio
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from soul_plugin_core.capabilities import CapabilityPolicy
from soul_plugin_core.contracts import PluginDefinition
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import CapabilityDeniedError, ConfigValidationError
from soul_plugin_core.host import PluginHost
from soul_plugin_core.instances import InstanceDefinition
from soul_plugin_core.lifecycle import FiberState
from soul_plugin_core.manifest import load_manifest


@pytest.fixture
def plugin_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.syspath_prepend(str(tmp_path))
    yield tmp_path
    for name in tuple(sys.modules):
        if name.startswith("m3c_test_"):
            sys.modules.pop(name, None)


def definition(
    root: Path,
    plugin_id: str,
    module_name: str,
    requests: tuple[str, ...] = (),
) -> PluginDefinition:
    plugin_dir = root / plugin_id
    plugin_dir.mkdir(parents=True, exist_ok=True)
    manifest = plugin_dir / "plugin.toml"
    manifest.write_text("# manifest source\n", encoding="utf-8")
    return PluginDefinition(
        plugin_id=plugin_id,
        version="1.0",
        api_version="1",
        entry_point=f"{module_name}:Plugin",
        plugin_dependencies=(),
        service_dependencies=(),
        provided_services=(),
        capability_requests=requests,
        config_schema=None,
        source=str(manifest),
    )


def make_host(
    root: Path,
    definitions: tuple[PluginDefinition, ...],
    *,
    policy: CapabilityPolicy | None = None,
    instances: tuple[InstanceDefinition, ...] | None = None,
    config: str = "",
) -> PluginHost:
    path = root / "config.toml"
    path.write_text(config, encoding="utf-8")
    return PluginHost(
        definitions,
        build_activation_plan(definitions),
        config_path=path,
        instances=instances,
        capability_policy=policy,
    )


def write_plugin(root: Path, name: str) -> None:
    (root / f"{name}.py").write_text(
        "SEEN = {}\n"
        "READ_ONLY = []\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        SEEN[context.instance_id] = context.capabilities\n"
        "        try:\n"
        "            context.capabilities = frozenset({'forged'})\n"
        "        except AttributeError:\n"
        "            READ_ONLY.append(True)\n"
        "    async def start(self):\n"
        "        return None\n"
        "    async def stop(self):\n"
        "        return None\n",
        encoding="utf-8",
    )


def test_default_policy_denies_requests_before_any_plugin_import(plugin_modules: Path) -> None:
    first = definition(plugin_modules, "a", "m3c_test_never_first")
    second = definition(
        plugin_modules,
        "b",
        "m3c_test_never_second",
        ("network.connect", "logging.write"),
    )
    host = make_host(plugin_modules, (first, second))
    with pytest.raises(CapabilityDeniedError) as caught:
        asyncio.run(host.activate())
    error = caught.value
    assert error.plugin_id == "b"
    assert error.instance_id == "b"
    assert error.phase == "capabilities"
    assert error.source == second.source
    assert error.missing == ("logging.write", "network.connect")
    assert "m3c_test_never_first" not in sys.modules
    assert "m3c_test_never_second" not in sys.modules
    assert host.get_state("a") is FiberState.PENDING
    assert host.get_state("b") is FiberState.PENDING


def test_manifest_request_flows_through_policy_before_activation(plugin_modules: Path) -> None:
    write_plugin(plugin_modules, "m3c_test_manifest")
    manifest = plugin_modules / "plugin.toml"
    manifest.write_text(
        "manifest_version = 1\n"
        "[plugin]\n"
        'id = "worker"\n'
        'version = "1.0"\n'
        'api_version = "1"\n'
        'entry_point = "m3c_test_manifest:Plugin"\n'
        "[capabilities]\n"
        'requests = ["logging.write"]\n',
        encoding="utf-8",
    )
    item = load_manifest(manifest)
    denied = make_host(plugin_modules, (item,))
    with pytest.raises(CapabilityDeniedError):
        asyncio.run(denied.activate())
    assert "m3c_test_manifest" not in sys.modules

    allowed = make_host(
        plugin_modules,
        (item,),
        policy=CapabilityPolicy({"worker": {"logging.write"}}),
    )

    async def scenario() -> None:
        await allowed.activate()
        assert sys.modules["m3c_test_manifest"].SEEN["worker"] == frozenset({"logging.write"})
        await allowed.stop()

    asyncio.run(scenario())


def test_grants_are_per_instance_immutable_and_limited_to_requests(plugin_modules: Path) -> None:
    write_plugin(plugin_modules, "m3c_test_worker")
    worker = definition(plugin_modules, "worker", "m3c_test_worker", ("logging.write",))
    policy = CapabilityPolicy(
        {
            "worker-eu": {"logging.write", "network.connect"},
            "worker-us": {"logging.write"},
            "not-deployed": {"unrelated"},
        }
    )
    instances = (
        InstanceDefinition("worker-us", "worker"),
        InstanceDefinition("worker-eu", "worker"),
    )
    host = make_host(plugin_modules, (worker,), policy=policy, instances=instances)

    async def scenario() -> None:
        await host.activate()
        module = sys.modules["m3c_test_worker"]
        assert module.SEEN == {
            "worker-eu": frozenset({"logging.write"}),
            "worker-us": frozenset({"logging.write"}),
        }
        assert module.READ_ONLY == [True, True]
        assert host.get_state("worker-eu") is FiberState.ACTIVE
        assert host.get_state("worker-us") is FiberState.ACTIVE
        await host.stop()

    asyncio.run(scenario())


def test_grant_for_plugin_id_does_not_authorize_different_instance(plugin_modules: Path) -> None:
    worker = definition(plugin_modules, "worker", "m3c_test_never_worker", ("network.connect",))
    host = make_host(
        plugin_modules,
        (worker,),
        policy=CapabilityPolicy({"worker": {"network.connect"}}),
        instances=(InstanceDefinition("worker-eu", "worker"),),
    )
    with pytest.raises(CapabilityDeniedError) as caught:
        asyncio.run(host.activate())
    assert caught.value.instance_id == "worker-eu"
    assert caught.value.missing == ("network.connect",)
    assert "m3c_test_never_worker" not in sys.modules


def test_missing_grant_for_one_instance_blocks_all_imports(plugin_modules: Path) -> None:
    worker = definition(plugin_modules, "worker", "m3c_test_never_multi", ("network.connect",))
    host = make_host(
        plugin_modules,
        (worker,),
        policy=CapabilityPolicy({"worker-eu": {"network.connect"}}),
        instances=(
            InstanceDefinition("worker-eu", "worker"),
            InstanceDefinition("worker-us", "worker"),
        ),
    )
    with pytest.raises(CapabilityDeniedError) as caught:
        asyncio.run(host.activate())
    assert caught.value.instance_id == "worker-us"
    assert "m3c_test_never_multi" not in sys.modules
    assert host.get_state("worker-eu") is FiberState.PENDING
    assert host.get_state("worker-us") is FiberState.PENDING


def test_policy_copies_grants_and_only_matches_exact_names(plugin_modules: Path) -> None:
    write_plugin(plugin_modules, "m3c_test_snapshot")
    worker = definition(plugin_modules, "worker", "m3c_test_snapshot", ("logging.write",))
    grants = {"worker": {"logging.write"}}
    policy = CapabilityPolicy(grants)
    grants["worker"].clear()
    grants["worker"].add("network.*")
    assert policy.grants_for("worker") == frozenset({"logging.write"})
    host = make_host(plugin_modules, (worker,), policy=policy)

    async def scenario() -> None:
        await host.activate()
        assert sys.modules["m3c_test_snapshot"].SEEN["worker"] == frozenset({"logging.write"})
        await host.stop()

    asyncio.run(scenario())


def test_wildcard_is_not_an_implicit_grant(plugin_modules: Path) -> None:
    worker = definition(plugin_modules, "worker", "m3c_test_never_wildcard", ("network.connect",))
    host = make_host(plugin_modules, (worker,), policy=CapabilityPolicy({"worker": {"network.*"}}))
    with pytest.raises(CapabilityDeniedError) as caught:
        asyncio.run(host.activate())
    assert caught.value.missing == ("network.connect",)
    assert "m3c_test_never_wildcard" not in sys.modules


def test_uninstantiated_plugin_does_not_require_grants(plugin_modules: Path) -> None:
    worker = definition(plugin_modules, "worker", "m3c_test_never_empty", ("network.connect",))
    host = make_host(plugin_modules, (worker,), instances=())
    asyncio.run(host.activate())
    assert "m3c_test_never_empty" not in sys.modules
    asyncio.run(host.dispose())


def test_configuration_error_precedes_capability_audit(plugin_modules: Path) -> None:
    worker = definition(plugin_modules, "worker", "m3c_test_never_config", ("network.connect",))
    host = make_host(plugin_modules, (worker,), config='[plugins.worker]\nsecret = "sensitive"\n')
    with pytest.raises(ConfigValidationError) as caught:
        asyncio.run(host.activate())
    assert "sensitive" not in str(caught.value)
    assert "m3c_test_never_config" not in sys.modules


@pytest.mark.parametrize(
    ("grants", "error"),
    [
        ({"worker": "logging.write"}, TypeError),
        ({"worker": {""}}, ValueError),
        ({"worker": {" logging.write"}}, ValueError),
        ({"": {"logging.write"}}, ValueError),
        ({"worker": {1}}, TypeError),
        ({1: {"logging.write"}}, TypeError),
    ],
)
def test_policy_rejects_invalid_input(grants: object, error: type[Exception]) -> None:
    with pytest.raises(error):
        CapabilityPolicy(grants)  # type: ignore[arg-type]
