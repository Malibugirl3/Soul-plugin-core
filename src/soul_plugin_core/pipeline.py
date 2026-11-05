"""同类型异步步骤的顺序调用组合。"""

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, init=False)
class AsyncPipeline(Generic[T]):
    """按顺序连接步骤；异常和取消继续传播，不回滚业务副作用。"""

    _steps: tuple[Callable[[T], Awaitable[T]], ...]

    def __init__(self, steps: Iterable[Callable[[T], Awaitable[T]]]) -> None:
        prepared = tuple(steps)
        if any(not callable(step) for step in prepared):
            raise TypeError("pipeline steps must be callable")
        object.__setattr__(self, "_steps", prepared)

    async def run(self, value: T) -> T:
        """依次等待每个步骤；空链原样返回输入。"""
        for step in self._steps:
            value = await step(value)
        return value
