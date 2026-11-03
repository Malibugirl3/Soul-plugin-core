# Soul Plugin Core

[中文说明](README.zh-CN.md) | English

A lightweight Python plugin core for composing long-running applications. It manages
plugin definitions, dependency plans, instances, services, and resource lifecycles.
Domain behavior, such as an Agent Runtime, belongs in the application built on top.

## Install and run the example

Python 3.10 or newer is required. From a repository checkout, activate your existing
`soul-plugin-core` Conda environment and install the project:

```powershell
conda activate soul-plugin-core
python -m pip install -e ".[dev]"
python examples/m4_demo/run_demo.py
```

If the environment does not exist yet, create it first with
`conda create -n soul-plugin-core python=3.11`. For application use without the
development tools, run `python -m pip install .` from the checkout instead.

The example loads three plugins, calls a service, publishes an event, and disposes
the host. See [the example source](examples/m4_demo/) for the complete manifests
and plugin implementations.

## Status

Manifest parsing, static dependency planning, deploy-config validation, and multi-instance activation are implemented.
One plugin can have several instances. Each instance has its own id, configuration, object, and cleanup scope.
Omitting the instance list keeps the single default instance, with `instance_id == plugin_id`.
Service handles are implemented: a plugin publishes in setup, and callers receive a typed async proxy bound to that provider generation.
Effect scopes support deferred cleanup, async context managers, child scopes, and managed background tasks.
Capability requests are checked against an immutable, per-instance host policy before plugin code is imported.
An in-process event facade provides scoped subscriptions and serial asynchronous delivery.
Durable events, the full effect system, and dynamic rebinding are not implemented. The public API is not stable.

The core manages plugin manifests, dependencies, activation order, context, services, and cleanup.
It does not implement an agent runtime, worker pool, or IPC.
Capability approval is an application-level audit, not a sandbox; in-process Python code can access system APIs directly.

## Write a minimal plugin

After installing the project, create these four files together in a separate directory.
The entry module must be importable by the Python process that starts the host.

`plugin.toml`:

```toml
manifest_version = 1

[plugin]
id = "example.hello"
version = "1.0"
api_version = "1"
entry_point = "hello_plugin:HelloPlugin"
```

`hello_plugin.py`:

```python
from collections.abc import Mapping
from soul_plugin_core.context import PluginContext


class HelloPlugin:
    async def setup(self, context: PluginContext, config: Mapping[str, object]) -> None:
        context.effects.defer(lambda: print("hello: cleanup"))

    async def start(self) -> None:
        print("hello: start")

    async def stop(self) -> None:
        print("hello: stop")
```

`config.toml`:

```toml
[plugins."example.hello"]
```

`run.py`:

```python
import asyncio
from soul_plugin_core.dependency import build_activation_plan
from soul_plugin_core.host import PluginHost
from soul_plugin_core.manifest import load_manifest


async def main() -> None:
    definition = load_manifest("plugin.toml")
    definitions = (definition,)
    plan = build_activation_plan(definitions)
    host = PluginHost(definitions, plan, config_path="config.toml")
    try:
        await host.activate()
    finally:
        await host.dispose()


if __name__ == "__main__":
    asyncio.run(main())
```

Run `python run.py` from that directory. The output is `hello: start`,
`hello: stop`, and `hello: cleanup`, in that order. All three lifecycle hooks must
be async, return `None`, and the entry class must have a no-argument constructor.
Register a resource's cleanup as soon as it is acquired: if `setup()` fails,
registered cleanup runs but `stop()` is not called.

## Manifests, configuration, and instances

Manifest version and `api_version` are currently `1`. Plugin and service versions
use PEP 440, and dependency constraints use PEP 440 specifiers. A manifest may also
contain `[dependencies]`, `[provides]`, `[capabilities]`, and `[config]`; see the
[clock](examples/m4_demo/manifests/clock/plugin.toml) and
[reporter](examples/m4_demo/manifests/reporter/plugin.toml) manifests for examples.

Strong dependencies must resolve and be acyclic. An optional dependency that is
missing, version-incompatible, or would create a cycle remains unbound in the plan.
Duplicate plugin IDs, duplicate service providers, and self-dependencies are errors.

The `config_path` file must exist. Its `[plugins."instance-id"]` tables are
validated before any plugin entry is imported. A plugin without `[config]` `schema`
accepts only an empty table. A declared schema is a JSON Schema Draft 2020-12 file
inside the manifest directory; configuration is recursively read-only after
validation, with lists exposed as tuples.

For example, to accept a `name` value, add this to the plugin manifest:

```toml
[config]
schema = "config.schema.json"
```

Put `config.schema.json` beside that manifest:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {"name": {"type": "string"}},
  "required": ["name"],
  "additionalProperties": false
}
```

Deployment can then set `name = "Soul"` under `[plugins."example.hello"]`, and
`setup()` can read `config["name"]`. `context.config` is the same read-only object.

Omitting `instances` creates one instance per plugin with the same ID. Passing an
explicit collection creates only those instances; an empty collection creates none.
When a dependency's provider has multiple instances, the consumer must select one
with `InstanceBindingSelection`, including for optional dependencies.

## Services and resources

`context.services.provide()`, `require()`, and `optional()` are available during
`setup()` only. A Provider declares its service in its manifest and stages it with
`provide()`; the Host makes it available after `start()` succeeds. Consumers declare
their dependencies and obtain a `ServiceHandle` during setup. `require()` is for
strong dependencies; `optional()` returns `None` only when the optional dependency
is unbound. The proxy exposes async methods declared on the supplied interface.
Handles are bound to the Provider generation and become unavailable when their
lease or Provider is closed. Business exceptions are propagated unchanged.

Use `context.effects.defer()`, `enter_async_context()`, `child_scope()`, and
`create_task()` for resources owned by an activation. Scope cleanup is LIFO;
managed tasks are cancelled and awaited on close. Task failures are reported during
cleanup unless already claimed via `TaskHandle.aclose()`; simply awaiting
`handle.task` does not consume the cleanup report. A normal `stop()` can be followed
by `activate()` for successfully stopped instances; `dispose()` is terminal. A
`FAILED` instance is not automatically retried, and there is no public per-instance
retry API yet.

The host grants capabilities by instance ID. Requests in a plugin manifest are required and are denied by default:

```python
from soul_plugin_core.capabilities import CapabilityPolicy
from soul_plugin_core.host import PluginHost
from soul_plugin_core.instances import InstanceDefinition

# `definitions` and `plan` include a plugin with ID "worker".
policy = CapabilityPolicy({"worker-us": {"logging.write"}})
host = PluginHost(
    definitions,
    plan,
    config_path="config.toml",
    instances=(InstanceDefinition("worker-us", "worker"),),
    capability_policy=policy,
)
```

`context.capabilities` is a read-only set containing only capabilities both requested and granted to that instance. The core does not yet provide gated network, file, or logging adapters.

For event notifications, declare and obtain `events.subscribe` and/or `events.publish`, then use the activation's event facade:

```python
from soul_plugin_core.context import EffectHandle, PluginContext


async def on_message(payload: object) -> None:
    print(payload)


def subscribe(context: PluginContext) -> EffectHandle:
    # A subscription made during setup becomes visible after start succeeds.
    return context.events.subscribe("message", on_message)


async def publish(context: PluginContext) -> None:
    await context.events.publish("message", "hello")
```

The returned handle can be closed early with `await subscription.aclose()`; the activation scope also unsubscribes on stop. Delivery is in-process and non-persistent. Each publication awaits listeners in order; ordinary listener failures are reported together as `EventDispatchError`. Stopping an instance waits for its active event work to finish. Service calls and event handlers must cooperate with shutdown; an operation that never returns can delay it indefinitely.

## Three-plugin example

From a repository checkout or an extracted source distribution, after installing the project in the `soul-plugin-core` Conda environment, run:

```powershell
python examples/m4_demo/run_demo.py
```

The example loads three manifests: `demo.clock` provides a service, `demo.reporter` consumes it and subscribes to an event, and `demo.emitter` publishes that event. The host grants the event capabilities by instance ID, activates in dependency order, and disposes in reverse order. Paths are resolved relative to the example, so the script can also be launched from another working directory.

## Runtime-shaped integration probe

`examples/soulrt_probe` exercises a local task entry, simulated model and tool services, streamed output, events, request cancellation, and cleanup. It is a test fixture, not an Agent Runtime or a production network protocol. Start the probe and a local client, or run its small benchmark:

```powershell
python examples/soulrt_probe/benchmark.py --requests 500 --concurrency 20
python examples/soulrt_probe/benchmark_tasks.py
```

The probe keeps each stream inside an async service call using a callback. The current service proxy does not track the lifetime of an async iterator after a method returns it; applications that require that interface need a separate ownership design.

## Development

```powershell
python -m pytest
python -m ruff check .
python -m ruff format --check .
python -m mypy

```

Use the existing Conda environment `soul-plugin-core`; do not create a project `.venv`.
For shells without activation, use `conda run -n soul-plugin-core python` in place of `python`.

Mypy currently checks the capability, dependency, instance-plan, configuration, lifecycle, context, service, and event modules, plus their type samples and the example, targeting Python 3.10.
This does not yet cover the entire package.

To build a source distribution and a wheel without changing the active environment (after installing the development dependencies):

```powershell
python -m build --no-isolation
```

The GitHub Actions workflow is configured to install and test the built wheel on Windows and Linux with Python 3.10 and 3.11. Its status is determined by the runs on GitHub.

## License

MIT License. See [LICENSE](LICENSE).
