"""M2a behavior acceptance tests; production lifecycle modules are implemented separately."""

import asyncio
import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from soul_plugin_core.contracts import PluginDefinition, PluginDependency
from soul_plugin_core.dependency import build_activation_plan


@pytest.fixture
def m2_api() -> SimpleNamespace:
    host = importlib.import_module("soul_plugin_core.host")
    lifecycle = importlib.import_module("soul_plugin_core.lifecycle")
    errors = importlib.import_module("soul_plugin_core.errors")
    return SimpleNamespace(
        PluginHost=host.PluginHost,
        FiberState=lifecycle.FiberState,
        ConfigValidationError=errors.ConfigValidationError,
        PluginLoadError=errors.PluginLoadError,
        PluginActivationError=errors.PluginActivationError,
        PluginCleanupError=errors.PluginCleanupError,
    )


def run(awaitable):
    return asyncio.run(awaitable)


def definition(
    root: Path,
    plugin_id: str,
    module_name: str,
    *,
    schema: str | None = "schema.json",
    dependencies: tuple[PluginDependency, ...] = (),
) -> PluginDefinition:
    plugin_dir = root / plugin_id
    plugin_dir.mkdir(parents=True, exist_ok=True)
    manifest = plugin_dir / "plugin.toml"
    manifest.write_text("# source marker\n", encoding="utf-8")
    return PluginDefinition(
        plugin_id=plugin_id,
        version="1.0",
        api_version="1",
        entry_point=f"{module_name}:Plugin",
        plugin_dependencies=dependencies,
        service_dependencies=(),
        provided_services=(),
        capability_requests=(),
        config_schema=schema,
        source=str(manifest),
    )


def write_schema(root: Path, plugin_id: str, schema: str) -> None:
    (root / plugin_id / "schema.json").write_text(schema, encoding="utf-8")


def write_module(root: Path, module_name: str, source: str) -> None:
    (root / f"{module_name}.py").write_text(source, encoding="utf-8")


def write_config(root: Path, text: str) -> Path:
    path = root / "config.toml"
    path.write_text(text, encoding="utf-8")
    return path


def make_host(m2_api, definitions, config_path: Path):
    definitions = tuple(definitions)
    return m2_api.PluginHost(
        definitions,
        build_activation_plan(definitions),
        config_path=config_path,
    )


def events(path: Path) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8").splitlines()


def plugin_source(
    event_path: Path,
    plugin_id: str,
    *,
    fail: str | None = None,
    two_cleanups: bool = False,
    cleanup_fails: bool = False,
    return_from: str | None = None,
) -> str:
    return f'''from pathlib import Path

EVENTS = Path({str(event_path)!r})
SEEN_CONFIG = None

def emit(value):
    with EVENTS.open("a", encoding="utf-8") as stream:
        stream.write(value + "\\n")

class Plugin:
    def __init__(self):
        emit("{plugin_id}:construct")
        {"raise RuntimeError('construct failed')" if fail == "construct" else "pass"}

    async def setup(self, context, config):
        global SEEN_CONFIG
        SEEN_CONFIG = config
        emit("{plugin_id}:setup")

        def cleanup_one():
            emit("{plugin_id}:cleanup-1")

        async def cleanup_two():
            emit("{plugin_id}:cleanup-2")
            {"raise RuntimeError('cleanup failed')" if cleanup_fails else "return None"}

        context.effects.defer(cleanup_one, label="cleanup-1")
        {"context.effects.defer(cleanup_two, label='cleanup-2')" if two_cleanups else ""}
        {"raise RuntimeError('setup failed')" if fail == "setup" else "pass"}
        {"return 'invalid'" if return_from == "setup" else "return None"}

    async def start(self):
        emit("{plugin_id}:start")
        {"raise RuntimeError('start failed')" if fail == "start" else "pass"}
        {"return 'invalid'" if return_from == "start" else "return None"}

    async def stop(self):
        emit("{plugin_id}:stop")
        {"raise RuntimeError('stop failed')" if fail == "stop" else "return None"}
'''


def empty_schema() -> str:
    return '{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false}'


def test_all_configs_are_validated_before_first_plugin_import(m2_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    first_sentinel = tmp_path / "first-imported.txt"
    second_sentinel = tmp_path / "second-imported.txt"
    write_module(
        tmp_path,
        "m2_first",
        f"from pathlib import Path\nPath({str(first_sentinel)!r}).write_text('imported')\nclass Plugin: pass\n",
    )
    write_module(
        tmp_path,
        "m2_second",
        f"from pathlib import Path\nPath({str(second_sentinel)!r}).write_text('imported')\nclass Plugin: pass\n",
    )
    first = definition(tmp_path, "first", "m2_first")
    second = definition(tmp_path, "second", "m2_second")
    schema = '{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}'
    write_schema(tmp_path, "first", schema)
    write_schema(tmp_path, "second", schema)
    config = write_config(
        tmp_path,
        '[plugins.first]\nvalue = 1\n[plugins.second]\nvalue = "invalid"\n',
    )

    host = make_host(m2_api, [first, second], config)
    with pytest.raises(m2_api.ConfigValidationError):
        run(host.activate())
    assert not first_sentinel.exists()
    assert not second_sentinel.exists()
    assert "m2_first" not in sys.modules
    assert "m2_second" not in sys.modules


@pytest.mark.parametrize(
    "config_text,schema",
    [
        ("[plugins.unknown]\nvalue = 1\n", "schema.json"),
        ("[plugins.demo]\nvalue = 1\n", None),
    ],
)
def test_unknown_or_unvalidated_config_fails_without_import(
    m2_api, tmp_path, monkeypatch, config_text, schema
):
    monkeypatch.syspath_prepend(str(tmp_path))
    sentinel = tmp_path / "imported.txt"
    write_module(
        tmp_path,
        "m2_config_guard",
        f"from pathlib import Path\nPath({str(sentinel)!r}).write_text('imported')\nclass Plugin: pass\n",
    )
    item = definition(tmp_path, "demo", "m2_config_guard", schema=schema)
    if schema is not None:
        write_schema(tmp_path, "demo", empty_schema())
    host = make_host(m2_api, [item], write_config(tmp_path, config_text))

    with pytest.raises(m2_api.ConfigValidationError):
        run(host.activate())
    assert not sentinel.exists()
    assert "m2_config_guard" not in sys.modules


def test_validated_config_is_deeply_read_only(m2_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(tmp_path, "m2_frozen", plugin_source(event_path, "demo"))
    item = definition(tmp_path, "demo", "m2_frozen")
    write_schema(
        tmp_path,
        "demo",
        '{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"nested":{"type":"object","properties":{"items":{"type":"array","items":{"type":"integer"}}},"required":["items"],"additionalProperties":false}},"required":["nested"],"additionalProperties":false}',
    )
    config = write_config(tmp_path, "[plugins.demo.nested]\nitems = [1, 2]\n")
    host = make_host(m2_api, [item], config)
    run(host.activate())

    seen = sys.modules["m2_frozen"].SEEN_CONFIG
    assert seen is not None
    assert seen["nested"]["items"] == (1, 2)
    with pytest.raises(TypeError):
        seen["new"] = "value"
    with pytest.raises(TypeError):
        seen["nested"]["items"][0] = 99
    run(host.stop())


def test_missing_schema_file_fails_before_plugin_import(m2_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    sentinel = tmp_path / "imported.txt"
    write_module(
        tmp_path,
        "m2_missing_schema",
        f"from pathlib import Path\nPath({str(sentinel)!r}).write_text('imported')\nclass Plugin: pass\n",
    )
    item = definition(tmp_path, "demo", "m2_missing_schema")
    host = make_host(m2_api, [item], write_config(tmp_path, "[plugins.demo]\n"))

    with pytest.raises(m2_api.ConfigValidationError):
        run(host.activate())
    assert not sentinel.exists()
    assert "m2_missing_schema" not in sys.modules


def test_activation_and_stop_follow_dependency_order(m2_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(tmp_path, "m2_provider", plugin_source(event_path, "provider"))
    write_module(tmp_path, "m2_consumer", plugin_source(event_path, "consumer"))
    provider = definition(tmp_path, "provider", "m2_provider")
    consumer = definition(
        tmp_path,
        "consumer",
        "m2_consumer",
        dependencies=(PluginDependency("provider", ">=1", False),),
    )
    for plugin_id in ("provider", "consumer"):
        write_schema(tmp_path, plugin_id, empty_schema())
    config = write_config(tmp_path, "[plugins.provider]\n[plugins.consumer]\n")
    host = make_host(m2_api, [consumer, provider], config)

    run(host.activate())
    assert events(event_path) == [
        "provider:construct",
        "provider:setup",
        "provider:start",
        "consumer:construct",
        "consumer:setup",
        "consumer:start",
    ]
    assert host.get_state("provider") is m2_api.FiberState.ACTIVE
    assert host.get_state("consumer") is m2_api.FiberState.ACTIVE

    run(host.stop())
    assert events(event_path)[-4:] == [
        "consumer:stop",
        "consumer:cleanup-1",
        "provider:stop",
        "provider:cleanup-1",
    ]
    assert host.get_state("provider") is m2_api.FiberState.PENDING
    assert host.get_state("consumer") is m2_api.FiberState.PENDING


@pytest.mark.parametrize("failure", ["construct", "setup"])
def test_construct_or_setup_failure_does_not_call_stop(m2_api, tmp_path, monkeypatch, failure):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    module_name = f"m2_{failure}_failure"
    write_module(tmp_path, module_name, plugin_source(event_path, "demo", fail=failure))
    item = definition(tmp_path, "demo", module_name)
    write_schema(tmp_path, "demo", empty_schema())
    host = make_host(m2_api, [item], write_config(tmp_path, "[plugins.demo]\n"))

    error_type = m2_api.PluginLoadError if failure == "construct" else m2_api.PluginActivationError
    with pytest.raises(error_type):
        run(host.activate())
    assert "demo:stop" not in events(event_path)
    if failure == "setup":
        assert events(event_path)[-1] == "demo:cleanup-1"
    assert host.get_state("demo") is m2_api.FiberState.FAILED


def test_start_failure_stops_failed_plugin_and_rolls_back_prior_plugins(
    m2_api, tmp_path, monkeypatch
):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(tmp_path, "m2_good", plugin_source(event_path, "good"))
    write_module(tmp_path, "m2_bad_start", plugin_source(event_path, "bad", fail="start"))
    good = definition(tmp_path, "good", "m2_good")
    bad = definition(
        tmp_path,
        "bad",
        "m2_bad_start",
        dependencies=(PluginDependency("good", ">=1", False),),
    )
    for plugin_id in ("good", "bad"):
        write_schema(tmp_path, plugin_id, empty_schema())
    host = make_host(
        m2_api,
        [bad, good],
        write_config(tmp_path, "[plugins.good]\n[plugins.bad]\n"),
    )

    with pytest.raises(m2_api.PluginActivationError):
        run(host.activate())
    assert events(event_path)[-4:] == [
        "bad:stop",
        "bad:cleanup-1",
        "good:stop",
        "good:cleanup-1",
    ]
    assert host.get_state("bad") is m2_api.FiberState.FAILED
    assert host.get_state("good") is m2_api.FiberState.PENDING


@pytest.mark.parametrize(
    "module_source",
    [
        "def Plugin():\n    return object()\n",
        (
            "class Plugin:\n"
            "    def setup(self, context, config): pass\n"
            "    async def start(self): pass\n"
            "    async def stop(self): pass\n"
        ),
    ],
)
def test_entry_must_be_class_with_async_hooks(m2_api, tmp_path, monkeypatch, module_source):
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(tmp_path, "m2_invalid_entry", module_source)
    item = definition(tmp_path, "demo", "m2_invalid_entry")
    write_schema(tmp_path, "demo", empty_schema())
    host = make_host(m2_api, [item], write_config(tmp_path, "[plugins.demo]\n"))

    with pytest.raises(m2_api.PluginLoadError):
        run(host.activate())
    assert host.get_state("demo") is m2_api.FiberState.FAILED


def test_effect_cleanup_is_lifo_and_continues_after_failure(m2_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(
        tmp_path,
        "m2_cleanup_failure",
        plugin_source(event_path, "demo", two_cleanups=True, cleanup_fails=True),
    )
    item = definition(tmp_path, "demo", "m2_cleanup_failure")
    write_schema(tmp_path, "demo", empty_schema())
    host = make_host(m2_api, [item], write_config(tmp_path, "[plugins.demo]\n"))
    run(host.activate())

    with pytest.raises(m2_api.PluginCleanupError) as caught:
        run(host.stop())
    assert events(event_path)[-3:] == [
        "demo:stop",
        "demo:cleanup-2",
        "demo:cleanup-1",
    ]
    assert isinstance(caught.value.errors, tuple)
    assert caught.value.errors
    assert host.get_state("demo") is m2_api.FiberState.FAILED


def test_stop_failure_does_not_block_other_instances_or_cleanup(m2_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(tmp_path, "m2_stop_good", plugin_source(event_path, "a"))
    write_module(tmp_path, "m2_stop_bad", plugin_source(event_path, "b", fail="stop"))
    first = definition(tmp_path, "a", "m2_stop_good")
    second = definition(tmp_path, "b", "m2_stop_bad")
    for plugin_id in ("a", "b"):
        write_schema(tmp_path, plugin_id, empty_schema())
    host = make_host(
        m2_api,
        [first, second],
        write_config(tmp_path, "[plugins.a]\n[plugins.b]\n"),
    )
    run(host.activate())

    with pytest.raises(m2_api.PluginCleanupError):
        run(host.stop())
    assert events(event_path)[-4:] == [
        "b:stop",
        "b:cleanup-1",
        "a:stop",
        "a:cleanup-1",
    ]
    assert host.get_state("b") is m2_api.FiberState.FAILED
    assert host.get_state("a") is m2_api.FiberState.PENDING


@pytest.mark.parametrize("return_from", ["setup", "start"])
def test_lifecycle_hooks_must_return_none(m2_api, tmp_path, monkeypatch, return_from):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    module_name = f"m2_return_{return_from}"
    write_module(
        tmp_path,
        module_name,
        plugin_source(event_path, "demo", return_from=return_from),
    )
    item = definition(tmp_path, "demo", module_name)
    write_schema(tmp_path, "demo", empty_schema())
    host = make_host(m2_api, [item], write_config(tmp_path, "[plugins.demo]\n"))

    with pytest.raises(m2_api.PluginActivationError):
        run(host.activate())
    assert host.get_state("demo") is m2_api.FiberState.FAILED


def test_stop_and_dispose_are_idempotent_and_dispose_is_terminal(m2_api, tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    event_path = tmp_path / "events.txt"
    write_module(tmp_path, "m2_idempotent", plugin_source(event_path, "demo"))
    item = definition(tmp_path, "demo", "m2_idempotent")
    write_schema(tmp_path, "demo", empty_schema())
    host = make_host(m2_api, [item], write_config(tmp_path, "[plugins.demo]\n"))
    run(host.activate())

    run(host.stop())
    first_events = events(event_path)
    run(host.stop())
    assert events(event_path) == first_events
    run(host.dispose())
    run(host.dispose())
    assert host.get_state("demo") is m2_api.FiberState.DISPOSED
    with pytest.raises(m2_api.PluginActivationError):
        run(host.activate())
    assert events(event_path) == first_events
