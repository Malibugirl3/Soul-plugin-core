"""Measure retention of results from completed managed tasks."""

import asyncio
import gc
import json
import time
import tracemalloc
import weakref

from soul_plugin_core.context import ActivationScope


class Payload:
    def __init__(self) -> None:
        self.data = bytearray(16_384)


async def main() -> None:
    scope = ActivationScope("bench", "bench")
    references: list[weakref.ReferenceType[Payload]] = []

    async def work() -> Payload:
        result = Payload()
        references.append(weakref.ref(result))
        return result

    tracemalloc.start()
    started = time.perf_counter()
    for _ in range(2000):
        await scope.create_task(work()).task
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    gc.collect()
    elapsed = time.perf_counter() - started
    current, peak = tracemalloc.get_traced_memory()
    started = time.perf_counter()
    errors = await scope.close()
    close_time = time.perf_counter() - started
    print(
        json.dumps(
            {
                "tasks": 2000,
                "payload_bytes": 16_384,
                "live_results": sum(reference() is not None for reference in references),
                "traced_mib": round(current / 1_048_576, 2),
                "peak_mib": round(peak / 1_048_576, 2),
                "elapsed_seconds": round(elapsed, 3),
                "close_ms": round(close_time * 1000, 2),
                "cleanup_errors": len(errors),
            }
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
