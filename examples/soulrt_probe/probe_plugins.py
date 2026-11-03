"""Small deterministic plugins for exercising a runtime-shaped integration."""

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable, Mapping
from typing import Protocol

from soul_plugin_core.context import PluginContext
from soul_plugin_core.errors import PluginActivationError

ChunkSink = Callable[[str], Awaitable[None]]


class Model(Protocol):
    async def plan(self, left: int, right: int, mode: str) -> tuple[int, int]: ...

    async def stream(self, value: int, mode: str, emit: ChunkSink) -> str: ...


class Tool(Protocol):
    async def add(self, left: int, right: int) -> int: ...


class Agent(Protocol):
    async def execute(
        self, request_id: str, left: int, right: int, mode: str, emit: ChunkSink
    ) -> str: ...


class ModelPlugin:
    async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
        context.services.provide("probe.model", self)

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def plan(self, left: int, right: int, mode: str) -> tuple[int, int]:
        if mode == "error":
            raise RuntimeError("model unavailable")
        return left, right

    async def stream(self, value: int, mode: str, emit: ChunkSink) -> str:
        if mode == "wait":
            await emit("started")
            await asyncio.Event().wait()
        result = str(value)
        for chunk in result:
            await emit(chunk)
        return result


class ToolPlugin:
    async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
        context.services.provide("probe.tool", self)

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def add(self, left: int, right: int) -> int:
        return left + right


class AgentPlugin:
    def __init__(self) -> None:
        self._context: PluginContext | None = None
        self._plan: Callable[[int, int, str], Awaitable[tuple[int, int]]] | None = None
        self._stream: Callable[[int, str, ChunkSink], Awaitable[str]] | None = None
        self._add: Callable[[int, int], Awaitable[int]] | None = None

    async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
        self._context = context
        model = context.services.require("probe.model", Model).service
        tool = context.services.require("probe.tool", Tool).service
        # Resolve the proxy methods once; their call gates still run on every invocation.
        self._plan, self._stream, self._add = model.plan, model.stream, tool.add
        context.services.provide("probe.agent", self)

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def execute(
        self, request_id: str, left: int, right: int, mode: str, emit: ChunkSink
    ) -> str:
        context, plan, stream, add = self._context, self._plan, self._stream, self._add
        if context is None or plan is None or stream is None or add is None:
            raise RuntimeError("agent is not ready")
        left, right = await plan(left, right, mode)
        await context.events.publish("probe.step", {"id": request_id, "step": "planned"})
        value = await add(left, right)
        await context.events.publish("probe.step", {"id": request_id, "step": "tool"})
        # Keep the stream inside the service call so the provider's in-flight
        # gate remains held until the last chunk or cancellation.
        result = await stream(value, mode, emit)
        await context.events.publish("probe.step", {"id": request_id, "step": "completed"})
        return result


class EntryPlugin:
    def __init__(self) -> None:
        self._context: PluginContext | None = None
        self._execute: Callable[[str, int, int, str, ChunkSink], Awaitable[str]] | None = None
        self._server: asyncio.AbstractServer | None = None
        self._steps: dict[str, list[str]] = {}

    async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
        self._context = context
        self._execute = context.services.require("probe.agent", Agent).service.execute
        context.events.subscribe("probe.step", self._on_step)

    async def start(self) -> None:
        if self._context is None:
            raise RuntimeError("entry is not ready")
        server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        self._server = server
        self._context.effects.defer(server.close, label="listener")
        sockets = server.sockets
        if not sockets:
            raise RuntimeError("listener has no socket")
        print(json.dumps({"type": "ready", "port": sockets[0].getsockname()[1]}), flush=True)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _on_step(self, payload: object) -> None:
        if isinstance(payload, dict):
            request_id, step = payload.get("id"), payload.get("step")
            if isinstance(request_id, str) and isinstance(step, str):
                steps = self._steps.get(request_id)
                if steps is not None:
                    steps.append(step)

    def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self._context is None:
            writer.close()
            return
        try:
            self._context.effects.create_task(self._serve(reader, writer), label="request")
        except PluginActivationError:
            # A connection can arrive while the listener is closing.
            writer.close()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        request_id = uuid.uuid4().hex

        async def send(message: Mapping[str, object]) -> None:
            writer.write((json.dumps(message) + "\n").encode("utf-8"))
            await writer.drain()

        async def emit(chunk: str) -> None:
            await send({"type": "chunk", "text": chunk})

        try:
            raw = await reader.readline()
            if not raw:
                return
            request: object = json.loads(raw)
            if not isinstance(request, dict):
                raise TypeError("request must be an object")
            if request.get("op") == "status":
                await send({"type": "status", "active": len(self._steps)})
                return
            left, right = request.get("left"), request.get("right")
            mode = request.get("mode", "normal")
            if (
                not isinstance(left, int)
                or isinstance(left, bool)
                or not isinstance(right, int)
                or isinstance(right, bool)
                or mode not in ("normal", "error", "wait")
            ):
                raise ValueError("invalid request")
            execute = self._execute
            if execute is None:
                raise RuntimeError("entry is not ready")
            self._steps[request_id] = []
            work = asyncio.ensure_future(execute(request_id, left, right, mode, emit))
            disconnect = asyncio.create_task(reader.read(1))
            try:
                finished, _ = await asyncio.wait(
                    (work, disconnect), return_when=asyncio.FIRST_COMPLETED
                )
                if disconnect in finished and work not in finished:
                    return
                result = await work
            finally:
                for task in (work, disconnect):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(work, disconnect, return_exceptions=True)
            await send({"type": "done", "result": result, "steps": self._steps[request_id]})
        except (ConnectionError, OSError):
            pass
        except Exception as exc:  # noqa: BLE001 - report probe failures to its test client
            try:
                await send({"type": "error", "message": str(exc)})
            except (ConnectionError, OSError):
                pass
        finally:
            self._steps.pop(request_id, None)
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
