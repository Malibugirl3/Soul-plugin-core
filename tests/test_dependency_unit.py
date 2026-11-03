from soul_plugin_core.contracts import (
    PluginDefinition,
    PluginDependency,
    ProvidedService,
    ServiceDependency,
)
from soul_plugin_core.dependency import build_activation_plan


def _plugin(
    plugin_id: str,
    *,
    version: str = "1.0",
    plugins: tuple[PluginDependency, ...] = (),
    services: tuple[ServiceDependency, ...] = (),
    provides: tuple[ProvidedService, ...] = (),
) -> PluginDefinition:
    return PluginDefinition(
        plugin_id=plugin_id,
        version=version,
        api_version="1",
        entry_point="not_imported:Plugin",
        plugin_dependencies=plugins,
        service_dependencies=services,
        provided_services=provides,
        capability_requests=(),
        config_schema=None,
        source=f"{plugin_id}/plugin.toml",
    )


def test_optional_missing_and_version_mismatch_are_distinct() -> None:
    missing = build_activation_plan(
        [_plugin("a", plugins=(PluginDependency("absent", ">=1", True),))]
    )
    mismatch = build_activation_plan(
        [
            _plugin("a", plugins=(PluginDependency("z", ">=2", True),)),
            _plugin("z"),
        ]
    )
    assert missing.unbound[0].reason == "missing"
    assert mismatch.unbound[0].reason == "version_mismatch"
    assert missing.unbound[0].cycle == ()
    assert mismatch.unbound[0].cycle == ()


def test_repeated_provider_edge_keeps_each_binding() -> None:
    provider = _plugin(
        "z",
        provides=(ProvidedService("clock", "1"), ProvidedService("store", "1")),
    )
    consumer = _plugin(
        "a",
        plugins=(PluginDependency("z", ">=1", False),),
        services=(
            ServiceDependency("clock", ">=1", False),
            ServiceDependency("store", ">=1", True),
        ),
    )
    plan = build_activation_plan([consumer, provider])
    assert plan.order == ("z", "a")
    assert len(plan.bindings) == 3
    assert plan.unbound == ()
