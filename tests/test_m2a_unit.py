"""M2a 内部行为：路径逃逸、已激活实例不回滚、清理范围。"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

from soul_plugin_core.config import validate_deployment
from soul_plugin_core.context import ActivationScope
from soul_plugin_core.contracts import PluginDefinition, PluginDependency
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.errors import ConfigValidationError, PluginActivationError, PluginCleanupError
from soul_plugin_core.host import PluginHost
from soul_plugin_core.lifecycle import FiberState

SCHEMA = (
    '{"$schema":"https://json-schema.org/draft/2020-12/schema",'
    '"type":"object","additionalProperties":false}'
)


def run(awaitable):
    return asyncio.run(awaitable)


def definition(
    root: Path,
    plugin_id: str,
    module_name: str,
    schema: str | None,
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


def test_schema_parent_path_cannot_escape_plugin_directory(tmp_path: Path) -> None:
    outside = tmp_path / "secret.json"
    outside.write_text("{}", encoding="utf-8")
    item = definition(tmp_path, "demo", "unused", "../secret.json")
    config = tmp_path / "config.toml"
    config.write_text("[plugins.demo]\n", encoding="utf-8")

    with pytest.raises(ConfigValidationError) as caught:
        validate_deployment({item.plugin_id: item}, config)
    assert "secret" not in str(caught.value)
    assert caught.value.plugin_id == "demo"


def link_directory(link: Path, target: Path) -> None:
    """Windows 用目录联接，POSIX 用目录符号链接。resolve 都会展开真实路径。"""
    if os.name == "nt":
        completed = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(target)],
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, completed.stderr
        return
    link.symlink_to(target, target_is_directory=True)


def test_schema_real_path_cannot_escape_through_junction(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "schema.json").write_text("{}", encoding="utf-8")
    item = definition(tmp_path, "demo", "unused", "linked/schema.json")
    link_directory(tmp_path / "demo" / "linked", outside)
    config = tmp_path / "config.toml"
    config.write_text("[plugins.demo]\n", encoding="utf-8")

    with pytest.raises(ConfigValidationError):
        validate_deployment({item.plugin_id: item}, config)


def test_config_error_does_not_echo_value(tmp_path: Path) -> None:
    item = definition(tmp_path, "demo", "unused", "schema.json")
    schema = (
        '{"$schema":"https://json-schema.org/draft/2020-12/schema",'
        '"type":"object","properties":{"token":{"type":"integer"}},'
        '"additionalProperties":false}'
    )
    (tmp_path / "demo" / "schema.json").write_text(schema, encoding="utf-8")
    config = tmp_path / "config.toml"
    config.write_text('[plugins.demo]\ntoken = "secret-token"\n', encoding="utf-8")

    with pytest.raises(ConfigValidationError) as caught:
        validate_deployment({item.plugin_id: item}, config)
    assert "secret-token" not in str(caught.value)


def test_scope_rejects_defer_after_close_and_cleanup_is_idempotent() -> None:
    calls: list[str] = []

    def sync_cleanup() -> None:
        calls.append("sync")

    async def async_cleanup() -> None:
        calls.append("async")

    scope = ActivationScope()
    scope.defer(sync_cleanup, label="sync")
    scope.defer(async_cleanup, label="async")
    assert run(scope.close()) == ()
    assert calls == ["async", "sync"]
    assert run(scope.close()) == ()
    assert calls == ["async", "sync"]
    with pytest.raises(PluginActivationError):
        scope.defer(sync_cleanup)


def test_already_active_instance_is_not_rolled_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    events = tmp_path / "events.txt"

    def source(plugin_id: str) -> str:
        return (
            "from pathlib import Path\n"
            f"EVENTS = Path({str(events)!r})\n"
            "def emit(value):\n"
            "    with EVENTS.open('a', encoding='utf-8') as stream:\n"
            "        stream.write(value + '\\n')\n"
            "class Plugin:\n"
            "    def __init__(self):\n"
            f"        emit({plugin_id!r} + ':construct')\n"
            "    async def setup(self, context, config):\n"
            f"        emit({plugin_id!r} + ':setup')\n"
            "    async def start(self):\n"
            f"        emit({plugin_id!r} + ':start')\n"
            "    async def stop(self):\n"
            f"        emit({plugin_id!r} + ':stop')\n"
        )

    (tmp_path / "m2_keep.py").write_text(source("keep"), encoding="utf-8")
    (tmp_path / "m2_retry.py").write_text(source("retry"), encoding="utf-8")
    keep = definition(tmp_path, "keep", "m2_keep", "schema.json")
    retry = definition(tmp_path, "retry", "m2_retry", "schema.json")
    for plugin_id in ("keep", "retry"):
        (tmp_path / plugin_id / "schema.json").write_text(SCHEMA, encoding="utf-8")
    config = tmp_path / "config.toml"
    config.write_text("[plugins.keep]\n[plugins.retry]\n", encoding="utf-8")
    definitions = (keep, retry)
    host = PluginHost(definitions, build_activation_plan(definitions), config_path=config)
    run(host.activate())

    host._fibers["retry"].state = FiberState.PENDING
    module = sys.modules["m2_retry"]

    async def broken_start(self: object) -> None:
        raise RuntimeError("start failed")

    module.Plugin.start = broken_start
    with pytest.raises(PluginActivationError):
        run(host.activate())

    assert host.get_state("keep") is FiberState.ACTIVE
    assert host.get_state("retry") is FiberState.FAILED
    assert "keep:stop" not in events.read_text(encoding="utf-8")


def write_config(root: Path, text: str) -> Path:
    path = root / "config.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_dispose_reaches_terminal_state_when_cleanup_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    events = tmp_path / "events.txt"

    def source(plugin_id: str, fail: bool) -> str:
        failure = "raise RuntimeError('cleanup failed')" if fail else "return None"
        return (
            "from pathlib import Path\n"
            f"EVENTS = Path({str(events)!r})\n"
            "def emit(value):\n"
            "    with EVENTS.open('a', encoding='utf-8') as stream:\n"
            "        stream.write(value + '\\n')\n"
            "class Plugin:\n"
            "    async def setup(self, context, config):\n"
            "        def cleanup():\n"
            f"            emit({plugin_id!r} + ':cleanup')\n"
            f"            {failure}\n"
            "        context.effects.defer(cleanup, label='cleanup')\n"
            "    async def start(self):\n"
            "        return None\n"
            "    async def stop(self):\n"
            f"        emit({plugin_id!r} + ':stop')\n"
            f"        {failure}\n"
        )

    (tmp_path / "m2_dispose_ok.py").write_text(source("ok", False), encoding="utf-8")
    (tmp_path / "m2_dispose_bad.py").write_text(source("bad", True), encoding="utf-8")
    ok = definition(tmp_path, "ok", "m2_dispose_ok", "schema.json")
    bad = definition(tmp_path, "bad", "m2_dispose_bad", "schema.json")
    for plugin_id in ("ok", "bad"):
        (tmp_path / plugin_id / "schema.json").write_text(SCHEMA, encoding="utf-8")
    definitions = (ok, bad)
    host = PluginHost(
        definitions,
        build_activation_plan(definitions),
        config_path=write_config(tmp_path, "[plugins.ok]\n[plugins.bad]\n"),
    )
    run(host.activate())

    with pytest.raises(PluginCleanupError):
        run(host.dispose())
    assert host.get_state("ok") is FiberState.DISPOSED
    assert host.get_state("bad") is FiberState.DISPOSED
    recorded = events.read_text(encoding="utf-8")
    assert "bad:stop" in recorded
    assert "bad:cleanup" in recorded
    assert "ok:stop" in recorded
    run(host.dispose())
    assert events.read_text(encoding="utf-8") == recorded


def test_activation_failure_keeps_every_cleanup_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    events = tmp_path / "events.txt"

    def source(plugin_id: str, fail_start: bool) -> str:
        start = "raise RuntimeError('start failed')" if fail_start else "return None"
        return (
            "from pathlib import Path\n"
            f"EVENTS = Path({str(events)!r})\n"
            "def emit(value):\n"
            "    with EVENTS.open('a', encoding='utf-8') as stream:\n"
            "        stream.write(value + '\\n')\n"
            "class Plugin:\n"
            "    async def setup(self, context, config):\n"
            "        def cleanup():\n"
            f"            emit({plugin_id!r} + ':defer')\n"
            f"            raise RuntimeError({plugin_id!r} + '-defer')\n"
            "        context.effects.defer(cleanup, label='defer')\n"
            "    async def start(self):\n"
            f"        {start}\n"
            "    async def stop(self):\n"
            f"        emit({plugin_id!r} + ':stop')\n"
            f"        raise RuntimeError({plugin_id!r} + '-stop')\n"
        )

    (tmp_path / "m2_rollback_good.py").write_text(source("good", False), encoding="utf-8")
    (tmp_path / "m2_rollback_bad.py").write_text(source("bad", True), encoding="utf-8")
    good = definition(tmp_path, "good", "m2_rollback_good", "schema.json")
    bad = definition(
        tmp_path,
        "bad",
        "m2_rollback_bad",
        "schema.json",
        (PluginDependency("good", ">=1", False),),
    )
    for plugin_id in ("good", "bad"):
        (tmp_path / plugin_id / "schema.json").write_text(SCHEMA, encoding="utf-8")
    definitions = (bad, good)
    host = PluginHost(
        definitions,
        build_activation_plan(definitions),
        config_path=write_config(tmp_path, "[plugins.good]\n[plugins.bad]\n"),
    )

    with pytest.raises(PluginActivationError) as caught:
        run(host.activate())
    assert [str(item) for item in caught.value.rollback_errors] == [
        "bad-stop",
        "bad-defer",
        "good-stop",
        "good-defer",
    ]
    assert caught.value.__cause__ is not None
    assert str(caught.value.__cause__) == "start failed"


@pytest.mark.parametrize("stage", ["setup", "start"])
def test_cancellation_closes_scope_and_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    events = tmp_path / "events.txt"
    module_name = f"m2_cancel_plugin_{stage}"
    support_name = f"m2_cancel_support_{stage}"
    (tmp_path / f"{module_name}.py").write_text(
        "import asyncio\n"
        f"import {support_name}\n"
        "from pathlib import Path\n"
        f"EVENTS = Path({str(events)!r})\n"
        "def emit(value):\n"
        "    with EVENTS.open('a', encoding='utf-8') as stream:\n"
        "        stream.write(value + '\\n')\n"
        "class Plugin:\n"
        "    async def setup(self, context, config):\n"
        "        def cleanup():\n"
        "            emit('cleanup')\n"
        "            raise RuntimeError('cleanup failed')\n"
        "        context.effects.defer(cleanup, label='cleanup')\n"
        f"        if {support_name}.stage == 'setup':\n"
        f"            {support_name}.ready.set()\n"
        "            await asyncio.sleep(30)\n"
        "    async def start(self):\n"
        f"        if {support_name}.stage == 'start':\n"
        f"            {support_name}.ready.set()\n"
        "            await asyncio.sleep(30)\n"
        "    async def stop(self):\n"
        "        emit('stop')\n",
        encoding="utf-8",
    )
    item = definition(tmp_path, "demo", module_name, "schema.json")
    (tmp_path / "demo" / "schema.json").write_text(SCHEMA, encoding="utf-8")
    host = PluginHost(
        (item,),
        build_activation_plan((item,)),
        config_path=write_config(tmp_path, "[plugins.demo]\n"),
    )

    async def main() -> None:
        support = ModuleType(support_name)
        ready = asyncio.Event()
        support.stage = stage
        support.ready = ready
        sys.modules[support_name] = support
        task = asyncio.create_task(host.activate())
        await asyncio.wait_for(ready.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    try:
        run(main())
    finally:
        sys.modules.pop(support_name, None)
        sys.modules.pop(module_name, None)
    fiber = host._fibers["demo"]
    assert fiber.state is FiberState.FAILED
    assert fiber.scope is None
    recorded = events.read_text(encoding="utf-8")
    assert "cleanup" in recorded
    assert ("stop" in recorded) is (stage == "start")


@pytest.mark.parametrize("stage", ["setup", "start"])
def test_cancellation_rolls_back_plugins_activated_in_this_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    events = tmp_path / "events.txt"
    support_name = f"m2_cancel_tx_support_{stage}"
    module_names = {
        "first": f"m2_cancel_tx_first_{stage}",
        "second": f"m2_cancel_tx_second_{stage}",
    }

    def source(plugin_id: str) -> str:
        waits = plugin_id == "second"
        header = "from pathlib import Path\n"
        if waits:
            header += f"import asyncio\nimport {support_name}\n"
        body = (
            f"EVENTS = Path({str(events)!r})\n"
            "def emit(value):\n"
            "    with EVENTS.open('a', encoding='utf-8') as stream:\n"
            "        stream.write(value + '\\n')\n"
            "class Plugin:\n"
            "    async def setup(self, context, config):\n"
            "        def cleanup():\n"
            f"            emit({plugin_id!r} + ':cleanup')\n"
            "        context.effects.defer(cleanup, label='cleanup')\n"
        )
        if waits:
            body += (
                f"        if {support_name}.stage == 'setup':\n"
                f"            {support_name}.ready.set()\n"
                "            await asyncio.sleep(30)\n"
            )
        body += "    async def start(self):\n"
        if waits:
            body += (
                f"        if {support_name}.stage == 'start':\n"
                f"            {support_name}.ready.set()\n"
                "            await asyncio.sleep(30)\n"
            )
        body += (
            "        return None\n    async def stop(self):\n"
            f"        emit({plugin_id!r} + ':stop')\n"
        )
        return header + body

    for plugin_id, module_name in module_names.items():
        (tmp_path / f"{module_name}.py").write_text(source(plugin_id), encoding="utf-8")
    first = definition(tmp_path, "first", module_names["first"], "schema.json")
    second = definition(
        tmp_path,
        "second",
        module_names["second"],
        "schema.json",
        (PluginDependency("first", ">=1", False),),
    )
    for plugin_id in ("first", "second"):
        (tmp_path / plugin_id / "schema.json").write_text(SCHEMA, encoding="utf-8")
    definitions = (second, first)
    host = PluginHost(
        definitions,
        build_activation_plan(definitions),
        config_path=write_config(tmp_path, "[plugins.first]\n[plugins.second]\n"),
    )

    async def main() -> None:
        support = ModuleType(support_name)
        ready = asyncio.Event()
        support.stage = stage
        support.ready = ready
        sys.modules[support_name] = support
        task = asyncio.create_task(host.activate())
        await asyncio.wait_for(ready.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    try:
        run(main())
    finally:
        sys.modules.pop(support_name, None)
        for module_name in module_names.values():
            sys.modules.pop(module_name, None)

    assert host.get_state("second") is FiberState.FAILED
    assert host.get_state("first") is FiberState.PENDING
    assert host._fibers["second"].scope is None
    assert host._fibers["first"].scope is None
    if stage == "setup":
        expected = ["second:cleanup", "first:stop", "first:cleanup"]
    else:
        expected = ["second:stop", "second:cleanup", "first:stop", "first:cleanup"]
    assert events.read_text(encoding="utf-8").splitlines() == expected
