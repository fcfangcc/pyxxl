import logging

from abc import ABC, abstractmethod
from typing import Any, AsyncContextManager, Optional

from pyxxl.types import LogRequest, LogResponse


MAX_LOG_TAIL_LINES = 1000


class LogBase(ABC):
    @abstractmethod
    def get_logger(self, log_id: int, *, stdout: bool = True, level: int = logging.INFO) -> logging.Logger:
        ...

    @abstractmethod
    async def get_logs(self, request: LogRequest, *, key: Optional[str] = None) -> LogResponse:
        ...

    @abstractmethod
    async def read_all(self, log_id: int, *, key: Optional[str] = None) -> str:
        ...

    @abstractmethod
    async def expired_once(self) -> None:
        ...

    @abstractmethod
    def mock_write(self, *lines: Any) -> AsyncContextManager[str]:
        ...

    @abstractmethod
    def mock_logger(self, log_id: int) -> AsyncContextManager["LogBase"]:
        ...
