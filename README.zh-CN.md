# Soul Plugin Core

中文 | [English](README.md)

Soul Plugin Core 是一个 Python 插件核心，负责插件声明、依赖计划、实例生命周期、服务协作和资源清理。Agent Runtime、任务调度、模型调用和进程管理属于构建在它之上的应用，例如 SoulRT。

项目目前处于早期开发阶段，公开 API 尚不稳定。

Core 还提供可选的类型化插件辅助、内存键值默认插件、静态插件集合和异步顺序调用链，方便构建小型应用。

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
- Schema 引用仅支持同一文档内的引用；对远程地址及其他文件的检索会被拒绝。配置错误的诊断不包含配置值。

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

`await host.deactivate(instance_id)` 显式停用实例，并按依赖逆序停止仍在运行的强依赖消费者；无关实例和共享 Provider 不受影响。所有声明和绑定都会保留。`await host.activate_instance(instance_id)` 在全部已绑定 Provider 均为 ACTIVE 时只恢复该实例；之后调用 `activate()` 可以启动满足条件的消费者，但不会自动恢复显式停用的实例。两种激活方式都预检全部声明实例，`FAILED` 实例不能通过此接口重试。

可以通过 `is_disabled()` 查询显式停用意图，`get_blockers()` 查询未运行的强依赖，`get_binding_blockers()` 查询全部未运行的已绑定 Provider。已绑定的 Optional 消费者在 Provider 停用时继续运行，但旧服务句柄失效；Provider 恢复后仍需重启消费者才能获得新句柄。目前不支持注销实例或动态重绑。

`host.instance_plan` 可检查不可变的实例计划；`host.snapshot()` 返回按计划顺序排列的不可变实例状态快照，包含状态、显式停用标记和 Provider 阻塞信息，不包含配置值或插件对象。它是同一事件循环内同步读取的瞬时快照，不承诺跨线程一致性；活动 Optional 消费者的 `binding_blockers` 可能非空，这不代表该消费者已经停止。

不要在插件生命周期钩子中等待同一 Host 的生命周期操作，也不要在服务调用、事件或扩展回调中等待会排空当前调用的停用或关闭操作；Core 会拒绝检测到的重入，避免循环等待。活跃调用创建的子任务继承这个限制，调用结束后限制解除。需要触发关闭时，应让调用先返回，再由宿主外层发起操作。

受管任务和资源清理也遵循相同限制。Core 无法识别所有通过外部 Task 或 Future 间接形成的等待链；例如，清理等待一个早已创建的任务，该任务又通过其他任务等待所属实例停用，仍可能死锁。关闭请求应交由不被该实例的清理过程等待的宿主外层任务执行。

## 服务、资源与事件

服务提供者在 Manifest 中声明服务，并在 `setup()` 中调用 `context.services.provide()` 暂存实现；`start()` 成功后宿主才提交服务。消费者也在 `setup()` 中获取服务：`require()` 用于强依赖，`optional()` 用于可选依赖；只有可选依赖在计划中未绑定时才返回 `None`。

`ServiceHandle.service` 是按接口类型提供的异步代理，调用会检查租约和 Provider 代次；业务异常保持原样。接口只暴露声明的异步方法。若异步方法返回一个流，方法返回后的迭代过程不会自动计入服务排空；需要流式输出时可以参考验证程序，在服务调用内部通过异步回调输出片段。

使用 `context.effects.defer()`、`enter_async_context()`、`child_scope()` 和 `create_task()` 管理本次激活拥有的资源。普通清理按后进先出执行；关闭时受管任务会被取消并等待。任务失败会在关闭时报告，除非已经通过 `TaskHandle.aclose()` 领取；仅仅 `await handle.task` 不会消除关闭时的错误报告。

事件通过 `context.events.subscribe()` 和 `publish()` 使用，插件必须在 Manifest 中申请对应的 `events.subscribe` 或 `events.publish`，并由宿主按实例 ID 授权。事件在进程内串行派发且不持久化；监听器错误会汇总为 `EventDispatchError`。事件和服务采用协作式排空，永不返回的调用可能使停止一直等待。

能力授权是应用层的声明和审计，并不是操作系统沙箱；进程内 Python 插件仍然可以直接调用系统 API。目前也没有受该策略控制的网络、文件或日志适配器。

## 插件类型与组合

`soul_plugin_core.plugins.PluginProtocol` 描述生命周期类型；`BasePlugin` 提供无操作的异步默认钩子；`ServicePlugin[T]` 在子类的 `create_service()` 中创建并暂存一个服务。这些辅助类都可以按需使用，现有插件不需要继承。服务名必须与 Manifest 声明一致，服务版本仍由 Manifest 决定；`create_service()` 取得资源后应立即登记清理。

`soul_plugin_core.builtin.memory_kv_manifest()` 返回内存键值插件的 Manifest 路径，不会导入插件入口。它提供 `core.kv` 服务，接口为 `soul_plugin_core.builtin.key_value.KeyValueService`，包含 `put`、`get`、`delete`；键和值均为字符串，缺失时 `get` 返回 `None`。每个插件实例的数据独立，停止该次激活时清空，数据不持久化。

`PluginBundle.from_manifests()` 收集 Manifest 路径；`compose_plugins()` 返回包含插件计划和实例计划的 `CompositionPlan`；`create_host()` 创建普通的未激活 Host。配置和授权仍在 `activate()` 中、导入插件入口前预检。插件集合不会自动授予权限，也不自动拥有其中的插件实例。

`AsyncPipeline[T]` 按顺序执行同类型的异步步骤。空调用链返回输入；异常和取消继续向上传播，不自动回滚业务副作用。通过 `ServiceHandle.service` 取得的方法每次调用仍会检查租约。

`ServiceSelector[T]` 可以在已经绑定的多个具名服务句柄之间选择策略或后端。默认项必须显式指定；请求的名称或默认项不存在时抛出 `ServiceSelectionError`，不会静默回退。它不负责动态注册服务，也不绕过静态依赖计划。

例如，两个策略分别以 `policy.first` 和 `policy.last` 声明服务，消费者在 `setup()` 中取得句柄并创建选择器：

```python
from soul_plugin_core.selection import ServiceSelector

# Policy 是应用自行定义的服务 Protocol。
first = context.services.require("policy.first", Policy)
last = context.services.require("policy.last", Policy)
selector = ServiceSelector({"first": first, "last": last}, default="first")
```

之后调用 `selector.select("last").service` 上的方法即可。两个 Provider 必须使用不同的服务名，且消费者的 Manifest 必须声明对应依赖。

观察型扩展通过 `context.extensions.register(name, observer)` 登记，通过 `await context.extensions.invoke(name, payload)` 按登记顺序调用；插件需要分别申请并获得 `extensions.register`、`extensions.invoke` 能力。setup 阶段的登记会等到插件启动成功后才生效，停止时先拒绝新调用、等待在途回调，再撤销登记。普通观察者异常汇总为 `EventDispatchError`。扩展通道与普通事件相互独立；目前不提供 `around` 包装、策略替换或运行中加载插件，不返回的回调可能使停止持续等待。

扩展点名称本身不建立插件依赖。如果调用方启动时必须已有观察者，应声明插件依赖以确定启动顺序。实例进入卸载状态后，其自身扩展门面不再接受登记或调用，`stop()` 中也不能使用该门面。

完整示例将默认键值插件、文本处理插件、工作流门面和消费者组合起来：

```powershell
python examples/plugin_types_demo/run_demo.py
```

示例的消费者只依赖门面服务。门面减少调用方对内部组织的了解，但不构成安全隔离或服务命名空间隔离。单实例在线卸载和动态重绑定尚未包含在这些辅助工具中。

## SoulRT 接入验证

[examples/soulrt_probe](examples/soulrt_probe/) 是独立测试夹具：它通过本地 TCP 接收模拟任务，组合模拟模型与工具服务，并验证流式输出、取消和清理。它不是真实 SoulRT，也不是生产网络协议。可运行：

```powershell
python examples/soulrt_probe/benchmark.py --requests 500 --concurrency 20
python examples/soulrt_probe/benchmark_tasks.py
python -m pytest tests/test_soulrt_integration_probe.py tests/test_soulrt_stream_gate.py -q
```

基准数据只反映本机模拟负载，不能代表真实模型性能。跨进程服务传输、动态重绑定、持久事件和强隔离目前未实现。

## 开发检查

开发使用短期 `codex/...` 分支，通过 PR 和 CI 后再合并到 `main`。
`main` 保存集成基线，版本标签标记正式发布；具体步骤见
[贡献与发布流程](CONTRIBUTING.md)。

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
