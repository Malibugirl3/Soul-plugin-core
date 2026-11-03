"""Black-box integration through a standalone runtime-shaped plugin fixture."""

import asyncio
import json
import os
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path

import soul_plugin_core

PROBE = Path(__file__).resolve().parents[1] / "examples" / "soulrt_probe" / "run_probe.py"
Scenario = Callable[[asyncio.subprocess.Process, int], Awaitable[None]]


async def stop_probe(process: asyncio.subprocess.Process) -> None:
    assert process.stdin is not None
    process.stdin.write(b"\n")
    await process.stdin.drain()
    stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=15)
    assert process.returncode == 0, stderr.decode()
    assert stderr == b"", stderr.decode()
    messages = [json.loads(line) for line in stdout.splitlines()]
    assert len(messages) == 1
    assert messages[0]["type"] == "closed"
    assert set(messages[0]["states"].values()) == {"DISPOSED"}


async def with_probe(tmp_path: Path, scenario: Scenario) -> None:
    env = os.environ.copy()
    assert soul_plugin_core.__file__ is not None
    package_root = str(Path(soul_plugin_core.__file__).resolve().parent.parent)
    inherited = env.get("PYTHONPATH")
    env["PYTHONPATH"] = os.pathsep.join([package_root, inherited] if inherited else [package_root])
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-u",
        str(PROBE),
        cwd=tmp_path,
        env=env,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert process.stdout is not None
        ready_line = await asyncio.wait_for(process.stdout.readline(), timeout=15)
        if not ready_line:
            assert process.stderr is not None
            raise AssertionError((await process.stderr.read()).decode())
        ready = json.loads(ready_line)
        assert ready["type"] == "ready"
        await asyncio.wait_for(scenario(process, ready["port"]), timeout=30)
        if process.returncode is None:
            await stop_probe(process)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


async def request(port: int, payload: dict[str, object]) -> list[dict[str, object]]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write((json.dumps(payload) + "\n").encode())
        await writer.drain()
        lines = await asyncio.wait_for(reader.read(), timeout=10)
        return [json.loads(line) for line in lines.splitlines()]
    finally:
        writer.close()
        await writer.wait_closed()


def test_external_requests_stream_results_and_recover_after_error(tmp_path: Path) -> None:
    async def scenario(_process: asyncio.subprocess.Process, port: int) -> None:
        error = await request(port, {"left": 1, "right": 2, "mode": "error"})
        assert error == [{"type": "error", "message": "model unavailable"}]

        messages = await request(port, {"left": 20, "right": 22})
        assert messages == [
            {"type": "chunk", "text": "4"},
            {"type": "chunk", "text": "2"},
            {"type": "done", "result": "42", "steps": ["planned", "tool", "completed"]},
        ]

    asyncio.run(with_probe(tmp_path, scenario))


def test_parallel_requests_keep_streams_and_events_separate(tmp_path: Path) -> None:
    async def scenario(_process: asyncio.subprocess.Process, port: int) -> None:
        responses = await asyncio.gather(
            *(request(port, {"left": i, "right": 100}) for i in range(30))
        )
        for index, messages in enumerate(responses):
            assert "".join(str(message["text"]) for message in messages[:-1]) == str(index + 100)
            assert messages[-1] == {
                "type": "done",
                "result": str(index + 100),
                "steps": ["planned", "tool", "completed"],
            }

    asyncio.run(with_probe(tmp_path, scenario))


def test_shutdown_cancels_active_stream_and_disposes_plugins(tmp_path: Path) -> None:
    async def scenario(process: asyncio.subprocess.Process, port: int) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            writer.write(b'{"left": 1, "right": 2, "mode": "wait"}\n')
            await writer.drain()
            chunk = json.loads(await asyncio.wait_for(reader.readline(), timeout=10))
            assert chunk == {"type": "chunk", "text": "started"}
            await stop_probe(process)
            assert await asyncio.wait_for(reader.read(), timeout=5) == b""
        finally:
            writer.close()
            await writer.wait_closed()

    asyncio.run(with_probe(tmp_path, scenario))


def test_client_disconnect_cancels_work_before_host_shutdown(tmp_path: Path) -> None:
    async def scenario(_process: asyncio.subprocess.Process, port: int) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b'{"left": 1, "right": 2, "mode": "wait"}\n')
        await writer.drain()
        assert json.loads(await asyncio.wait_for(reader.readline(), timeout=10)) == {
            "type": "chunk",
            "text": "started",
        }
        assert await request(port, {"op": "status"}) == [{"type": "status", "active": 1}]
        writer.close()
        await writer.wait_closed()

        async def wait_for_cancel() -> None:
            while await request(port, {"op": "status"}) != [{"type": "status", "active": 0}]:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_cancel(), timeout=5)
        messages = await request(port, {"left": 1, "right": 2})
        assert messages[-1]["result"] == "3"

    asyncio.run(with_probe(tmp_path, scenario))
