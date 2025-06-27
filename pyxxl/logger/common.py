import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Optional

from pyxxl.ctx import g
from pyxxl.types import LogRequest, LogResponse

MAX_LOG_TAIL_LINES = 1000
TASK_FORMAT = (
    "%(asctime)s.%(msecs)03d [%(threadName)s] [%(logId)s] "
    "%(levelname)s %(pathname)s(%(funcName)s:%(lineno)d) - %(message)s"
)
TASKDATE_FORMAT = "%Y-%m-%d %H:%M:%S"
TASK_FORMATTER = logging.Formatter(TASK_FORMAT, datefmt=TASKDATE_FORMAT)


class LogBase(ABC):
    executor_logger: logging.Logger
    NOT_FOUND_LOGS = "No such logid logs."

    @abstractmethod
    def get_logger(
        self,
        log_id: int,
        *,
        stdout: bool = True,
        level: int = logging.INFO,
        expired_seconds: Optional[int] = None,
    ) -> logging.Logger: ...

    @abstractmethod
    async def get_logs(self, request: LogRequest) -> LogResponse: ...

    @abstractmethod
    async def read_task_logs(self, log_id: int) -> str | None:
        """一次性读取某个log id的所有日志,主要用于单测"""
        ...

    async def expired_once(self, **kwargs: Any) -> bool:
        """执行一次批量过期操作,如果是redis啥的自带过期就无需实现此方法"""
        return False

    async def expired_loop(self, seconds: int = 3600) -> None:
        """
        Args:
            seconds (int, optional): one loop seconds. Defaults to 3600.
        """
        self.executor_logger.debug("start expired_loop...")
        try:
            while True:
                await self.expired_once()
                await asyncio.sleep(seconds)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.executor_logger.exception(e)
        finally:
            self.executor_logger.info("expired_loop exit...")

    def after_running(self, logger: logging.Logger) -> None:
        return None


class PyxxlFileHandler(logging.FileHandler):
    def emit(self, record: Any) -> None:
        xxl_kwargs = g.try_get_run_data()
        record.logId = xxl_kwargs.logId if xxl_kwargs else "NotInTask"
        return super().emit(record)


class PyxxlStreamHandler(logging.StreamHandler):
    def emit(self, record: Any) -> None:
        xxl_kwargs = g.try_get_run_data()
        record.logId = xxl_kwargs.logId if xxl_kwargs else "NotInTask"
        return super().emit(record)
