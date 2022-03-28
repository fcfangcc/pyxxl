from asyncio import iscoroutinefunction
from typing import Callable, Optional
from dataclasses import dataclass


@dataclass
class HandlerInfo:
    handler: Callable

    @property
    def is_async(self) -> bool:
        return iscoroutinefunction(self.handler)


@dataclass(frozen=True)
class RunData:
    jobId: int
    logId: int
    executorHandler: str
    executorBlockStrategy: str

    executorParams: Optional[str] = None
    executorTimeout: Optional[int] = None
    logDateTime: Optional[int] = None
    glueType: Optional[str] = None
    glueSource: Optional[str] = None
    glueUpdatetime: Optional[int] = None
    broadcastIndex: Optional[int] = None
    broadcastTotal: Optional[int] = None
