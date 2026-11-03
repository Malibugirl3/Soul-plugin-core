"""mypy 样例：enter_async_context 与 create_task 保留类型变量。"""

from types import TracebackType

from soul_plugin_core.context import ActivationScope, EffectHandle, TaskHandle


class Box:
    async def __aenter__(self) -> int:
        return 1

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None


async def _work() -> str:
    return "ok"


def _cleanup() -> None:
    return None


async def _sample(scope: ActivationScope) -> tuple[int, str, str | None]:
    handle: TaskHandle[str] = scope.create_task(_work(), label="work")
    value: str = await handle.task
    entered: int = await scope.enter_async_context(Box(), label="box")
    effect: EffectHandle = scope.defer(_cleanup, label="cleanup")
    return entered, value, effect.label
