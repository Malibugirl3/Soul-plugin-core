"""Measure a local mock workload; the result is not an LLM or IPC benchmark."""

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

PROBE = Path(__file__).with_name("run_probe.py")


async def benchmark(count: int, concurrency: int) -> None:
    started = time.perf_counter()
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-u",
        str(PROBE),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert process.stdout is not None
        ready = json.loads(await asyncio.wait_for(process.stdout.readline(), timeout=15))
        port = int(ready["port"])
        startup = time.perf_counter() - started
        latencies: list[float] = []
        work = iter(range(count))

        async def worker() -> None:
            for index in work:
                begin = time.perf_counter()
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                try:
                    writer.write((json.dumps({"left": index, "right": 42}) + "\n").encode())
                    await writer.drain()
                    response = await asyncio.wait_for(reader.read(), timeout=10)
                    messages = [json.loads(line) for line in response.splitlines()]
                    if not messages or messages[-1].get("result") != str(index + 42):
                        raise RuntimeError("incorrect probe response")
                    latencies.append((time.perf_counter() - begin) * 1000)
                finally:
                    writer.close()
                    await writer.wait_closed()

        elapsed_start = time.perf_counter()
        await asyncio.gather(*(worker() for _ in range(concurrency)))
        elapsed = time.perf_counter() - elapsed_start
        latencies.sort()

        def percentile(fraction: float) -> float:
            return round(
                latencies[min(len(latencies) - 1, int((len(latencies) - 1) * fraction))], 2
            )

        assert process.stdin is not None
        process.stdin.write(b"\n")
        await process.stdin.drain()
        shutdown_start = time.perf_counter()
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=15)
        if process.returncode != 0 or stderr or json.loads(stdout)["type"] != "closed":
            raise RuntimeError(f"probe shutdown failed: {stderr.decode()}")
        print(
            json.dumps(
                {
                    "requests": count,
                    "concurrency": concurrency,
                    "startup_ms": round(startup * 1000, 2),
                    "shutdown_ms": round((time.perf_counter() - shutdown_start) * 1000, 2),
                    "requests_per_second": round(count / elapsed, 2),
                    "latency_ms_p50": percentile(0.50),
                    "latency_ms_p95": percentile(0.95),
                    "latency_ms_p99": percentile(0.99),
                }
            )
        )
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=300)
    parser.add_argument("--concurrency", type=int, default=20)
    args = parser.parse_args()
    if args.requests < 1 or args.concurrency < 1:
        parser.error("requests and concurrency must be positive")
    asyncio.run(benchmark(args.requests, args.concurrency))
