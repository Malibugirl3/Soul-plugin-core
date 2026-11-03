# SoulRT-shaped integration probe

This independent fixture uses the public Core modules to simulate an external task
entry, an agent, a model, and a tool. It uses deterministic local implementations:
no model API, GPU, real SoulRT worker, or cross-process service transport is involved.
Its loopback JSON-line protocol is for testing only and has no authentication.

With the `soul-plugin-core` Conda environment active, start a complete client/server
workload and report latency and throughput:

```powershell
python examples/soulrt_probe/benchmark.py --requests 500 --concurrency 20
python examples/soulrt_probe/benchmark_tasks.py
python -m pytest tests/test_soulrt_integration_probe.py -q
```

For manual use, run `python examples/soulrt_probe/run_probe.py`. It prints a JSON
`ready` line containing its dynamically allocated localhost TCP port; press Enter
to stop it. Each connection may send one JSON line such as
`{"left": 20, "right": 22}` and receive JSON lines for each streamed chunk and a
final result. Keep the connection open while awaiting the response; disconnecting
cancels the pending request. `mode` may be `normal`, `error`, or `wait` for fault
injection. A request `{"op": "status"}` reports the active request count.

The model emits chunks through an awaited callback while its async service method
is still running. This keeps the service in flight until streaming ends or is
cancelled. Returning an async iterator from the service would not provide that
guarantee with the current Core proxy.
