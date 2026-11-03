"""Contract acceptance tests; the resolver is intentionally not implemented here."""

import importlib
import itertools
import sys
from dataclasses import replace

import pytest

from soul_plugin_core.contracts import (
    PluginDefinition,
    PluginDependency,
    ProvidedService,
    ServiceDependency,
)
from soul_plugin_core.manifest import load_manifest


@pytest.fixture
def resolver():
    # Keep existing tests collectible while reporting missing implementation as errors.
    return importlib.import_module("soul_plugin_core.dependency").build_activation_plan


def plugin(name, *, version="1.0", plugins=(), services=(), provides=()):
    return PluginDefinition(
        plugin_id=name,
        version=version,
        api_version="1",
        entry_point="not_imported:Plugin",
        plugin_dependencies=tuple(PluginDependency(*item) for item in plugins),
        service_dependencies=tuple(ServiceDependency(*item) for item in services),
        provided_services=tuple(ProvidedService(*item) for item in provides),
        capability_requests=(),
        config_schema=None,
        source=f"{name}/plugin.toml",
    )


def binding_rows(plan):
    return tuple((b.consumer_id, b.kind, b.target, b.provider_id) for b in plan.bindings)


def unbound_rows(plan):
    return tuple((b.consumer_id, b.kind, b.target, b.reason, tuple(b.cycle)) for b in plan.unbound)


def snapshot(plan):
    return tuple(plan.order), binding_rows(plan), unbound_rows(plan)


def expect_error(resolver, definitions, *, owner=None, target=None):
    error_type = importlib.import_module("soul_plugin_core.errors").DependencyResolutionError
    with pytest.raises(error_type) as caught:
        resolver(definitions)
    error = caught.value
    assert error.source
    assert error.field or error.path
    if owner is not None:
        assert error.plugin_id == owner.plugin_id
        assert error.source == owner.source
    if target is not None:
        assert target in str(error)
    return error


def test_empty_plan(resolver):
    assert snapshot(resolver([])) == ((), (), ())


def test_independent_plugins_have_stable_order(resolver):
    definitions = [plugin("z"), plugin("a"), plugin("m")]
    for permutation in itertools.permutations(definitions):
        assert snapshot(resolver(permutation)) == (("a", "m", "z"), (), ())


def test_mixed_diamond_orders_providers_before_consumers(resolver):
    definitions = [
        plugin("a", provides=(("clock", "1.0"),)),
        plugin("b", services=(("clock", ">=1", False),)),
        plugin("c", plugins=(("a", ">=1", False),)),
        plugin("d", plugins=(("b", ">=1", False), ("c", ">=1", False))),
    ]
    expected = None
    for permutation in itertools.permutations(definitions):
        plan = resolver(permutation)
        assert plan.order == ("a", "b", "c", "d")
        assert set(binding_rows(plan)) == {
            ("b", "service", "clock", "a"),
            ("c", "plugin", "a", "a"),
            ("d", "plugin", "b", "b"),
            ("d", "plugin", "c", "c"),
        }
        expected = snapshot(plan) if expected is None else expected
        assert snapshot(plan) == expected


def test_multiple_bindings_share_one_graph_edge(resolver):
    provider = plugin("z", provides=(("clock", "1"), ("store", "1")))
    consumer = plugin(
        "a",
        plugins=(("z", ">=1", False),),
        services=(("clock", ">=1", False), ("store", ">=1", True)),
    )
    plan = resolver([consumer, provider])
    assert plan.order == ("z", "a")
    assert len(plan.bindings) == 3
    assert plan.unbound == ()


@pytest.mark.parametrize("kind", ["plugin", "service"])
@pytest.mark.parametrize("incompatible", [False, True])
def test_required_dependency_failure(resolver, kind, incompatible):
    kwargs = {"plugins" if kind == "plugin" else "services": (("target", ">=2", False),)}
    consumer = plugin("consumer", **kwargs)
    definitions = [consumer]
    if incompatible:
        definitions.append(
            plugin("target")
            if kind == "plugin"
            else plugin("provider", provides=(("target", "1"),))
        )
    expect_error(resolver, definitions, owner=consumer, target="target")


def test_duplicate_plugin_ids_fail(resolver):
    expect_error(resolver, [plugin("a"), plugin("a", version="2")], target="a")


@pytest.mark.parametrize("optional", [False, True])
def test_global_provider_conflict_is_never_masked(resolver, optional):
    definitions = [
        plugin("a", provides=(("clock", "1"),)),
        plugin("b", provides=(("clock", "2"),)),
        plugin("c", services=(("clock", ">=1,<2", optional),)),
    ]
    for permutation in itertools.permutations(definitions):
        expect_error(resolver, permutation, target="clock")


def test_unused_provider_conflict_still_fails(resolver):
    expect_error(
        resolver,
        [plugin("a", provides=(("clock", "1"),)), plugin("b", provides=(("clock", "1"),))],
        target="clock",
    )


@pytest.mark.parametrize("kind", ["plugin", "service"])
@pytest.mark.parametrize("optional", [False, True])
@pytest.mark.parametrize("constraint", [">=1", ">=99"])
def test_self_dependency_rejected_before_optional_or_version_filter(
    resolver, kind, optional, constraint
):
    kwargs = {"plugins" if kind == "plugin" else "services": (("self", constraint, optional),)}
    definition = plugin("self", provides=(("self", "1"),), **kwargs)
    expect_error(resolver, [definition], owner=definition, target="self")


def test_required_cycle_fails_with_closed_path(resolver):
    definitions = [
        plugin("a", plugins=(("b", ">=1", False),)),
        plugin("b", services=(("clock", ">=1", False),)),
        plugin("c", plugins=(("a", ">=1", False),), provides=(("clock", "1"),)),
    ]
    for permutation in itertools.permutations(definitions):
        error = expect_error(resolver, permutation)
        assert error.path[0] == error.path[-1]
        assert set(error.path) == {"a", "b", "c"}


@pytest.mark.parametrize("kind", ["plugin", "service"])
@pytest.mark.parametrize("incompatible", [False, True])
def test_optional_unavailable_is_diagnosed(resolver, kind, incompatible):
    kwargs = {"plugins" if kind == "plugin" else "services": (("target", ">=2", True),)}
    definitions = [plugin("consumer", **kwargs)]
    if incompatible:
        definitions.append(
            plugin("target")
            if kind == "plugin"
            else plugin("provider", provides=(("target", "1"),))
        )
    plan = resolver(definitions)
    assert plan.bindings == ()
    assert len(plan.unbound) == 1
    record = plan.unbound[0]
    assert (record.consumer_id, record.kind, record.target) == ("consumer", kind, "target")
    assert record.reason and record.reason != "cycle_masked"
    assert record.cycle == ()


@pytest.mark.parametrize("kind", ["plugin", "service"])
def test_compatible_optional_creates_binding_and_order(resolver, kind):
    kwargs = {"plugins" if kind == "plugin" else "services": (("z", ">=1", True),)}
    plan = resolver([plugin("a", **kwargs), plugin("z", provides=(("z", "1"),))])
    assert plan.order == ("z", "a")
    assert binding_rows(plan) == (("a", kind, "z", "z"),)
    assert plan.unbound == ()


@pytest.mark.parametrize("kind", ["plugin", "service"])
def test_optional_cycle_masks_only_closing_binding(resolver, kind):
    kwargs = {"plugins" if kind == "plugin" else "services": (("a", ">=1", True),)}
    definitions = [
        plugin("a", plugins=(("b", ">=1", False),), provides=(("a", "1"),)),
        plugin("b", **kwargs),
    ]
    before = tuple(definitions)
    for permutation in itertools.permutations(definitions):
        plan = resolver(permutation)
        assert plan.order == ("b", "a")
        assert binding_rows(plan) == (("a", "plugin", "b", "b"),)
        assert len(plan.unbound) == 1
        record = plan.unbound[0]
        assert (record.consumer_id, record.kind, record.target, record.reason) == (
            "b",
            kind,
            "a",
            "cycle_masked",
        )
        assert record.cycle[0] == record.cycle[-1]
        assert set(record.cycle) == {"a", "b"}
    assert tuple(definitions) == before
    assert definitions[1].plugin_dependencies or definitions[1].service_dependencies


def test_all_optional_cycle_is_deterministic_without_masking_every_edge(resolver):
    definitions = [
        plugin("a", plugins=(("b", ">=1", True),)),
        plugin("b", plugins=(("c", ">=1", True),)),
        plugin("c", plugins=(("a", ">=1", True),)),
    ]
    snapshots = [snapshot(resolver(p)) for p in itertools.permutations(definitions)]
    assert all(result == snapshots[0] for result in snapshots)
    assert len(snapshots[0][1]) == 2
    assert len(snapshots[0][2]) == 1
    assert snapshots[0][2][0][3] == "cycle_masked"
    assert set(snapshots[0][2][0][4]) == {"a", "b", "c"}


@pytest.mark.parametrize(
    ("version", "constraint", "matches"),
    [
        ("1.5", ">=1,<2", True),
        ("2.0a1", ">=1,<3", False),
        ("2.0a1", ">=2.0a1,<3", True),
        ("1.0+local", "==1.0", True),
    ],
)
def test_pep440_matching_for_plugin_and_service(resolver, version, constraint, matches):
    consumer = plugin(
        "a", plugins=(("z", constraint, True),), services=(("clock", constraint, True),)
    )
    provider = plugin("z", version=version, provides=(("clock", version),))
    plan = resolver([consumer, provider])
    assert len(plan.bindings) == (2 if matches else 0)
    assert len(plan.unbound) == (0 if matches else 2)


def test_plan_and_records_are_immutable(resolver):
    plan = resolver(
        [plugin("a", plugins=(("z", ">=1", False), ("absent", ">=1", True))), plugin("z")]
    )
    assert isinstance(plan.order, tuple)
    assert isinstance(plan.bindings, tuple)
    assert isinstance(plan.unbound, tuple)
    for obj, field, value in [
        (plan, "order", ()),
        (plan.bindings[0], "provider_id", "other"),
        (plan.unbound[0], "reason", "other"),
    ]:
        with pytest.raises((AttributeError, TypeError)):
            setattr(obj, field, value)


@pytest.mark.parametrize("failure", [False, True])
def test_toml_to_plan_never_executes_entry_point(resolver, tmp_path, monkeypatch, failure):
    module_name = "dependency_acceptance_sentinel"
    sentinel = tmp_path / "executed.txt"
    (tmp_path / f"{module_name}.py").write_text(
        f"from pathlib import Path\nPath({str(sentinel)!r}).write_text('executed')\n"
        "raise RuntimeError('entry point executed')\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    definitions = []
    for name in ("a", "b"):
        path = tmp_path / f"{name}.toml"
        dependency = "missing" if failure else "b"
        text = (
            "manifest_version = 1\n[plugin]\n"
            f'id = "{name}"\nversion = "1.0"\napi_version = "1"\n'
            f'entry_point = "{module_name}:Plugin"\n'
        )
        if name == "a":
            text += f'[dependencies]\nplugins = [{{id="{dependency}",version=">=1"}}]\n'
        path.write_text(text, encoding="utf-8")
        definitions.append(load_manifest(path))
    try:
        if failure:
            expect_error(resolver, definitions, owner=definitions[0], target="missing")
        else:
            assert resolver(definitions).order == ("b", "a")
    finally:
        assert not sentinel.exists()
        assert module_name not in sys.modules


def test_reordering_dependency_declarations_does_not_change_plan(resolver):
    consumer = plugin("a", plugins=(("b", ">=1", True), ("c", ">=1", True)))
    reordered = replace(consumer, plugin_dependencies=tuple(reversed(consumer.plugin_dependencies)))
    providers = [plugin("b"), plugin("c")]
    assert snapshot(resolver([consumer, *providers])) == snapshot(resolver([reordered, *providers]))
