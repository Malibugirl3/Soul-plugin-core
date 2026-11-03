"""mypy 样例：ServiceHandle.service 保留 interface 上的成员。"""

from typing import Protocol

from soul_plugin_core.services import ServiceFacade, ServiceHandle


class Clock(Protocol):
    async def now(self) -> int: ...


def _service(handle: ServiceHandle[Clock]) -> Clock:
    return handle.service


class ConcreteClock:
    async def now(self) -> int:
        return 0


async def _call(facade: ServiceFacade) -> int:
    required = facade.require("clock", Clock)
    concrete = facade.require("clock", ConcreteClock)
    optional = facade.optional("clock", Clock)
    if optional is not None:
        await optional.service.now()
    await concrete.service.now()
    return await _service(required).now()
