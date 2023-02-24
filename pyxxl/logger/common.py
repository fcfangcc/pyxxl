import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncContextManager, Optional

from pyxxl.types import LogRequest, LogResponse

logger = logging.getLogger(__name__)


MAX_LOG_TAIL_LINES = 1000


class LogBase(ABC):
    @abstractmethod
    def get_logger(self, log_id: int, *, stdout: bool = True, level: int = logging.INFO) -> logging.Logger:
        ...

    @abstractmethod
    async def get_logs(self, request: LogRequest, *, key: Optional[str] = None) -> LogResponse:
        ...

    @abstractmethod
    async def read_task_logs(self, log_id: int, *, key: Optional[str] = None) -> str:
        """一次性读取某个log id的所有日志,主要用于单测"""
        ...

    @abstractmethod
    def mock_write(self, *lines: Any) -> AsyncContextManager[str]:
        ...

    @abstractmethod
    def mock_logger(self, log_id: int) -> AsyncContextManager["LogBase"]:
        ...

    async def expired_once(self) -> None:  # noqa: B027
        """执行一次批量过期操作,如果是redis啥的自带过期就无需实现此方法"""
        pass

    async def expired_loop(self, seconds: int = 3600) -> None:
        """
        Args:
            seconds (int, optional): one loop seconds. Defaults to 3600.
        """
        logger.debug("start expired_loop...")
        while True:
            await self.expired_once()
            await asyncio.sleep(seconds)
