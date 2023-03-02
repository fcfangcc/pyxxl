import logging
import os
import time
from contextlib import asynccontextmanager
from logging import FileHandler
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncGenerator, List

import aiofiles

from pyxxl.types import LogRequest, LogResponse
from pyxxl.utils import STD_FORMATTER

from .common import LogBase

if TYPE_CHECKING:
    from logging import Handler


LOG_NAME_PREFIX = "pyxxl-{log_id}.log"
LOG_NAME_REGEX = "pyxxl-*.log"
MAX_LOG_TAIL_LINES = 1000
logger = logging.getLogger(__name__)


class XXLogger:
    """为了可以关闭文件写入流"""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def __del__(self) -> None:
        # !!! StreamHandler remove会有问题，需要判断是否是FileHandler
        for h in self._logger.handlers:
            if isinstance(h, logging.FileHandler):
                logger.debug("close file log object: {}.".format(h))
                h.close()
                self._logger.removeHandler(h)

    def __getattr__(self, name: str) -> Any:
        if name in ["info", "error", "warning", "debug", "exception", "handlers"]:
            return getattr(self._logger, name)


class DiskLog(LogBase):
    def __init__(self, log_path: str, log_tail_lines: int = 0, expired_days: int = 14) -> None:
        self.log_path = Path(log_path)
        if not self.log_path.exists():
            self.log_path.mkdir()  # pragma: no cover
            logger.info("create logdir %s" % self.log_path)  # pragma: no cover
        self.log_tail_lines = log_tail_lines or MAX_LOG_TAIL_LINES
        self.expired_days = expired_days

    def key(self, log_id: int) -> str:
        return self.log_path.joinpath(LOG_NAME_PREFIX.format(log_id=log_id)).absolute().as_posix()

    def get_logger(  # type:ignore[override]
        self, log_id: int, *, stdout: bool = True, level: int = logging.INFO
    ) -> XXLogger:
        logger = logging.getLogger("pyxxl-task-{%s}" % log_id)
        logger.propagate = False
        logger.setLevel(level)
        handlers: list[Handler] = [logging.StreamHandler()] if stdout else []
        handlers.append(FileHandler(self.key(log_id), delay=True))
        for h in handlers:
            h.setFormatter(STD_FORMATTER)
            h.setLevel(level)
            logger.addHandler(h)
        return XXLogger(logger)

    async def get_logs(self, request: LogRequest, *, key: str = None) -> LogResponse:
        # todo: 优化获取中间行的逻辑，缓存之前每行日志的大小然后直接seek
        logs = ""
        to_line_num = request["fromLineNum"]  # start with 1
        is_end = False
        key = key or self.key(request["logId"])
        try:
            async with aiofiles.open(key, mode="r") as f:
                for i in range(1, request["fromLineNum"] + self.log_tail_lines):
                    log = await f.readline()
                    if log == "":
                        is_end = True
                        break
                    elif i >= request["fromLineNum"]:
                        to_line_num = i
                        logs += log
        except FileNotFoundError as e:
            logger.warning(str(e), exc_info=True)
            logs = "No such logid logs."

        return LogResponse(
            fromLineNum=request["fromLineNum"],
            toLineNum=to_line_num,
            logContent=logs,
            isEnd=is_end,
        )

    async def read_task_logs(self, log_id: int, *, key: str = None) -> str:
        key = key or self.key(log_id)
        async with aiofiles.open(key, mode="r") as f:
            return await f.read()

    async def expired_once(self) -> None:
        now = time.time()
        expire_timestamp = now - 3600 * 24 * self.expired_days
        del_list: List[Path] = []
        if self.log_path.exists():
            for sub_path in [i for i in self.log_path.glob(LOG_NAME_REGEX) if i.is_file()]:
                ctime = os.path.getctime(sub_path.absolute())
                if ctime < expire_timestamp:
                    del_list.append(sub_path)

        if del_list:
            logger.info("delete expired logs [{}] - {}".format(len(del_list), " | ".join(str(i) for i in del_list)))
            for i in del_list:
                i.unlink()

    @asynccontextmanager
    async def mock_write(self, *lines: Any) -> AsyncGenerator[str, None]:
        async with aiofiles.tempfile.NamedTemporaryFile() as f:
            await f.writelines(lines)
            await f.flush()
            await f.seek(0)
            yield str(f.name)

    @asynccontextmanager
    async def mock_logger(self, _log_id: int) -> AsyncGenerator[LogBase, None]:
        async with aiofiles.tempfile.TemporaryDirectory() as d:
            handler = DiskLog(log_path=d)
            yield handler
