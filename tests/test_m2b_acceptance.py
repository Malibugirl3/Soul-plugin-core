"""M2b multi-instance acceptance tests; production implementation is separate."""

import importlib
from types import SimpleNamespace

import pytest

from soul_plugin_core.contracts import (
    PluginDefinition,
    PluginDependency,
    ProvidedService,
    ServiceDependency,
)
from soul_plugin_core.dependency import build_activation_plan


@pytest.fixture
def m2b_api() -> SimpleNamespace:
    instances = importlib.import_module("soul_plugin_core.instances")
    errors = importlib.import_module("soul_plugin_core.errors")
    return SimpleNamespace(
        InstanceDefinition=instances.InstanceDefinition,
        InstanceBindingSelection=instances.InstanceBindingSelection,
        InstanceResolutionError=errors.InstanceResolutionError,
        expand_instance_plan=instances.expand_instance_plan,
    )


def plugin(
    plugin_id: str,
    *,
    plugins: tuple[tuple[str, bool], ...] = (),
    services: tuple[tuple[str, bool], ...] = (),
    provides: tuple[str, ...] = (),
) -> PluginDefinition:
    return PluginDefinition(
        plugin_id=plugin_id,
        version="1.0",
        api_version="1",
        entry_point="not_imported:Plugin",
        plugin_dependencies=tuple(
            PluginDependency(target, ">=1", optional) for target, optional in plugins
        ),
        service_dependencies=tuple(
            ServiceDependency(target, ">=1", optional) for target, optional in services
        ),
        provided_services=tuple(ProvidedService(name, "1.0") for name in provides),
        capability_requests=(),
        config_schema=None,
        source=f"{plugin_id}/plugin.toml",
    )


def expand(m2b_api, definitions, instances):
    definitions = tuple(definitions)
    return m2b_api.expand_instance_plan(
        definitions,
        build_activation_plan(definitions),
        instances,
    )


def binding_snapshot(plan):
    return tuple(
        (item.consumer_instance_id, item.kind, item.target, item.provider_instance_id)
        for item in plan.bindings
    )


def test_none_preserves_m2a_default_instances(m2b_api):
    definitions = (plugin("a"), plugin("b", plugins=(("a", False),)))

    plan = expand(m2b_api, definitions, None)

    assert plan.order == ("a", "b")
    assert binding_snapshot(plan) == (("b", "plugin", "a", "a"),)


def test_explicit_empty_deployment_does_not_add_defaults(m2b_api):
    plan = expand(m2b_api, (plugin("a"),), ())

    assert plan.order == ()
    assert plan.bindings == ()
    assert plan.unbound == ()


def test_two_instances_of_same_plugin_are_distinct_and_deterministic(m2b_api):
    item = plugin("worker")
    declarations = (
        m2b_api.InstanceDefinition("worker-us", "worker"),
        m2b_api.InstanceDefinition("worker-eu", "worker"),
    )

    first = expand(m2b_api, (item,), declarations)
    second = expand(m2b_api, (item,), reversed(declarations))

    assert first == second
    assert first.order == ("worker-eu", "worker-us")


def test_unique_provider_instance_is_selected_automatically(m2b_api):
    provider = plugin("provider", provides=("clock",))
    consumer = plugin("consumer", services=(("clock", False),))
    declarations = (
        m2b_api.InstanceDefinition("consumer-a", "consumer"),
        m2b_api.InstanceDefinition("consumer-b", "consumer"),
        m2b_api.InstanceDefinition("clock-main", "provider"),
    )

    plan = expand(m2b_api, (consumer, provider), declarations)

    assert binding_snapshot(plan) == (
        ("consumer-a", "service", "clock", "clock-main"),
        ("consumer-b", "service", "clock", "clock-main"),
    )
    assert plan.order.index("clock-main") < plan.order.index("consumer-a")
    assert plan.order.index("clock-main") < plan.order.index("consumer-b")


@pytest.mark.parametrize("optional", [False, True])
def test_multiple_provider_instances_require_explicit_selection(m2b_api, optional):
    provider = plugin("provider", provides=("clock",))
    consumer = plugin("consumer", services=(("clock", optional),))
    declarations = (
        m2b_api.InstanceDefinition("clock-a", "provider"),
        m2b_api.InstanceDefinition("clock-b", "provider"),
        m2b_api.InstanceDefinition("consumer", "consumer"),
    )

    with pytest.raises(m2b_api.InstanceResolutionError) as caught:
        expand(m2b_api, (consumer, provider), declarations)

    assert caught.value.source == "instance-declaration"
    assert caught.value.instance_id == "consumer"


def test_explicit_provider_selection_is_stable(m2b_api):
    provider = plugin("provider", provides=("clock",))
    consumer = plugin("consumer", services=(("clock", False),))
    selection = m2b_api.InstanceBindingSelection("service", "clock", "clock-b")
    declarations = (
        m2b_api.InstanceDefinition("clock-b", "provider"),
        m2b_api.InstanceDefinition("consumer", "consumer", (selection,)),
        m2b_api.InstanceDefinition("clock-a", "provider"),
    )

    first = expand(m2b_api, (consumer, provider), declarations)
    second = expand(m2b_api, (provider, consumer), reversed(declarations))

    assert first == second
    assert binding_snapshot(first) == (("consumer", "service", "clock", "clock-b"),)


@pytest.mark.parametrize(
    "selection",
    [
        ("service", "clock", "other"),
        ("plugin", "provider", "consumer"),
        ("service", "unknown", "clock-a"),
    ],
)
def test_invalid_or_unused_selection_fails_before_runtime(m2b_api, selection):
    provider = plugin("provider", provides=("clock",))
    consumer = plugin("consumer", services=(("clock", False),))
    other = plugin("other")
    chosen = m2b_api.InstanceBindingSelection(*selection)
    declarations = (
        m2b_api.InstanceDefinition("clock-a", "provider"),
        m2b_api.InstanceDefinition("consumer", "consumer", (chosen,)),
        m2b_api.InstanceDefinition("other", "other"),
    )

    with pytest.raises(m2b_api.InstanceResolutionError):
        expand(m2b_api, (consumer, provider, other), declarations)


def test_selection_cannot_restore_m1_unbound_optional(m2b_api):
    consumer = plugin("consumer", services=(("missing", True),))
    selection = m2b_api.InstanceBindingSelection("service", "missing", "consumer")
    declarations = (m2b_api.InstanceDefinition("consumer", "consumer", (selection,)),)

    with pytest.raises(m2b_api.InstanceResolutionError):
        expand(m2b_api, (consumer,), declarations)


@pytest.mark.parametrize(
    "declarations",
    [
        (("same", "a"), ("same", "a")),
        (("unknown", "missing"),),
        ((" ", "a"),),
    ],
)
def test_invalid_instance_declarations_fail(m2b_api, declarations):
    items = tuple(m2b_api.InstanceDefinition(*item) for item in declarations)

    with pytest.raises(m2b_api.InstanceResolutionError) as caught:
        expand(m2b_api, (plugin("a"),), items)

    assert caught.value.source == "instance-declaration"
