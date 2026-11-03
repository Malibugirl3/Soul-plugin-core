# Soul Plugin Core

中文 | [English](README.md)

Soul Plugin Core 是一个 Python 插件核心，负责插件声明、依赖计划、实例生命周期、服务协作和资源清理。Agent Runtime、任务调度、模型调用和进程管理属于构建在它之上的应用，例如 SoulRT。

项目目前处于早期开发阶段，公开 API 尚不稳定。

## 安装与运行示例

需要 Python 3.10 或更新版本。在项目目录中使用已有的 Conda 环境：

```powershell
conda activate soul-plugin-core
python -m pip install -e ".[dev]"
python examples/m4_demo/run_demo.py
```

如果环境尚不存在，可以先运行 `conda create -n soul-plugin-core python=3.11`。只作为应用依赖使用时，可以在项目目录执行 `python -m pip install .`，不必安装开发工具。

示例会加载三个插件，完成服务调用、事件通知和宿主清理。完整代码和 Manifest 位于 [examples/m4_demo](examples/m4_demo/)。

## 从零编写一个插件

安装项目后，在另一个目录中创建下面四个文件。插件模块必须能被运行宿主的 Python 进程导入。

`plugin.toml` 声明身份与入口：

```toml
manifest_version = 1

[plugin]
id = "example.hello"
version = "1.0"
api_version = "1"
entry_point = "hello_plugin:HelloPlugin"
```

`hello_plugin.py` 实现生命周期：

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

`config.toml` 保存部署配置。本例没有声明配置 Schema，因此只使用空表：

```toml
[plugins."example.hello"]
```

`run.py` 解析 Manifest、建立计划并运行宿主：

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

在这四个文件所在的目录执行 `python run.py`，依次输出 `hello: start`、`hello: stop` 和 `hello: cleanup`。

入口类必须能够无参数构造；`setup`、`start`、`stop` 都必须是异步方法并返回 `None`。取得资源后应立即登记清理：如果 `setup()` 中途失败，已登记的清理会执行，但宿主不会调用尚未完成初始化的插件的 `stop()`。

## Manifest、依赖与配置

- 当前 Manifest 版本与插件 `api_version` 均为 `1`。插件和服务版本使用 PEP 440，依赖版本约束使用 PEP 440 说明符。
- `[dependencies]` 可以声明插件或服务依赖，`[provides]` 声明提供的服务。强依赖缺失、版本不匹配或成环会使计划失败；可选依赖缺失、版本不匹配或成环时会在计划中标记为未绑定。重复插件 ID、重复服务 Provider 和自依赖均报错。
- 部署配置文件必须存在，配置表使用 `[plugins."实例 ID"]`。插件没有声明 `[config]` 中的 `schema` 时，只接受空配置。
- Schema 是相对 Manifest 目录的 JSON 文件，使用 JSON Schema Draft 2020-12；解析后的真实路径必须位于该目录内。宿主在导入任何插件入口前校验全部实例配置和能力授权。校验成功的配置递归只读，列表会变成元组。

可参考 [服务提供者 Manifest](examples/m4_demo/manifests/clock/plugin.toml) 和 [服务消费者 Manifest](examples/m4_demo/manifests/reporter/plugin.toml)。

例如，需要允许 `name` 配置时，在 `plugin.toml` 中增加：

```toml
[config]
schema = "config.schema.json"
```

在 Manifest 同一目录创建 `config.schema.json`：

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {"name": {"type": "string"}},
  "required": ["name"],
  "additionalProperties": false
}
```

此时可以在部署配置的 `[plugins."example.hello"]` 表中写入 `name = "Soul"`，并在 `setup()` 中通过 `config["name"]` 读取。`context.config` 与 `setup` 参数是同一份只读对象。

## 实例与生命周期

构造 `PluginHost` 时不传 `instances`，每个插件默认创建一个同名实例；传入实例集合后只创建声明过的实例，空集合表示不创建实例。多个 Provider 实例可用时，消费者必须通过 `InstanceBindingSelection` 指定使用哪一个，Optional 依赖也一样。

`activate()` 按依赖顺序启动。启动失败或取消时，宿主逆序回滚本次新启动的实例；调用前已经运行的实例不受这次回滚影响。`stop()` 逆序停止，成功停止的实例可以再次激活。`dispose()` 是终态，即使清理失败也不能再次激活。进入 `FAILED` 的实例不会自动重试，目前没有公开的单实例重试 API。

`PluginHost.get_state(instance_id)` 可以读取实例状态。入口类及服务通过各自模块导入；包根当前只导出 `__version__`。

## 服务、资源与事件

服务提供者在 Manifest 中声明服务，并在 `setup()` 中调用 `context.services.provide()` 暂存实现；`start()` 成功后宿主才提交服务。消费者也在 `setup()` 中获取服务：`require()` 用于强依赖，`optional()` 用于可选依赖；只有可选依赖在计划中未绑定时才返回 `None`。

`ServiceHandle.service` 是按接口类型提供的异步代理，调用会检查租约和 Provider 代次；业务异常保持原样。接口只暴露声明的异步方法。若异步方法返回一个流，方法返回后的迭代过程不会自动计入服务排空；需要流式输出时可以参考验证程序，在服务调用内部通过异步回调输出片段。

使用 `context.effects.defer()`、`enter_async_context()`、`child_scope()` 和 `create_task()` 管理本次激活拥有的资源。普通清理按后进先出执行；关闭时受管任务会被取消并等待。任务失败会在关闭时报告，除非已经通过 `TaskHandle.aclose()` 领取；仅仅 `await handle.task` 不会消除关闭时的错误报告。

事件通过 `context.events.subscribe()` 和 `publish()` 使用，插件必须在 Manifest 中申请对应的 `events.subscribe` 或 `events.publish`，并由宿主按实例 ID 授权。事件在进程内串行派发且不持久化；监听器错误会汇总为 `EventDispatchError`。事件和服务采用协作式排空，永不返回的调用可能使停止一直等待。

能力授权是应用层的声明和审计，并不是操作系统沙箱；进程内 Python 插件仍然可以直接调用系统 API。目前也没有受该策略控制的网络、文件或日志适配器。

## SoulRT 接入验证

[examples/soulrt_probe](examples/soulrt_probe/) 是独立测试夹具：它通过本地 TCP 接收模拟任务，组合模拟模型与工具服务，并验证流式输出、取消和清理。它不是真实 SoulRT，也不是生产网络协议。可运行：

```powershell
python examples/soulrt_probe/benchmark.py --requests 500 --concurrency 20
python examples/soulrt_probe/benchmark_tasks.py
python -m pytest tests/test_soulrt_integration_probe.py tests/test_soulrt_stream_gate.py -q
```

基准数据只反映本机模拟负载，不能代表真实模型性能。跨进程服务传输、动态重绑定、持久事件和强隔离目前未实现。

## 开发检查

```powershell
python -m pytest
python -m mypy
python -m ruff check .
python -m ruff format --check .
python -m build --no-isolation
```

这些命令使用上面安装的开发依赖。CI 配置覆盖 Windows/Linux 与 Python 3.10/3.11；以 GitHub Actions 的实际运行结果为准。

## 许可证

本项目使用 [MIT License](LICENSE)。
