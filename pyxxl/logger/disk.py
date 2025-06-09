import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncGenerator, List, Optional

import aiofiles

from pyxxl.log import executor_logger
from pyxxl.types import LogRequest, LogResponse

from .common import TASK_FORMATTER, LogBase, PyxxlFileHandler, PyxxlStreamHandler

if TYPE_CHECKING:
    from logging import Handler


LOG_NAME_PREFIX = "pyxxl-{log_id}.log"
LOG_NAME_REGEX = "pyxxl-*.log"
MAX_LOG_TAIL_LINES = 1000


class DiskLog(LogBase):
    def __init__(
        self,
        log_path: str,
        log_tail_lines: int = 0,
        expired_days: float = 14,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.log_path = Path(log_path)
        self.executor_logger = logger or executor_logger
        if not self.log_path.exists():
            self.log_path.mkdir()  # pragma: no cover
            self.executor_logger.info("create logdir %s" % self.log_path)  # pragma: no cover
        self.log_tail_lines = log_tail_lines or MAX_LOG_TAIL_LINES
        self.expired_seconds = round(3600 * 24 * expired_days)

    def key(self, log_id: int) -> str:
        return self.log_path.joinpath(LOG_NAME_PREFIX.format(log_id=log_id)).absolute().as_posix()

    def get_logger(self, log_id: int, *, stdout: bool = True, level: int = logging.INFO) -> logging.Logger:
        logger = logging.getLogger("pyxxl.task_log.disk.task-{%s}" % log_id)
        logger.propagate = False
        logger.setLevel(level)
        handlers: list[Handler] = [PyxxlStreamHandler()] if stdout else []
        handlers.append(PyxxlFileHandler(self.key(log_id), delay=True))
        for h in handlers:
            h.setFormatter(TASK_FORMATTER)
            h.setLevel(level)
            logger.addHandler(h)
        return logger

    async def get_logs(self, request: LogRequest, *, key: Optional[str] = None) -> LogResponse:
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
            self.executor_logger.warning(str(e), exc_info=True)
            logs = "No such logid logs."

        return LogResponse(
            fromLineNum=request["fromLineNum"],
            toLineNum=to_line_num,
            logContent=logs,
            isEnd=is_end,
        )

    async def read_task_logs(self, log_id: int, *, key: Optional[str] = None) -> str:
        key = key or self.key(log_id)
        async with aiofiles.open(key, mode="r") as f:
            return await f.read()

    async def expired_once(self, batch: int = 1000) -> None:
        now = time.time()
        expire_timestamp = now - self.expired_seconds

        del_list = await asyncio.to_thread(self._scan_expired_files, expire_timestamp)
        self.executor_logger.info("Search expired logs, found %s", len(del_list))

        for i, p in enumerate(del_list, 1):
            if i % batch == 0:
                self.executor_logger.debug("Delete expired logs step: %s", i)
                await asyncio.sleep(0.01)  # release CPU for other tasks
            p.unlink(missing_ok=True)

        if del_list:
            self.executor_logger.info("Delete expired logs successfully, count: %s", len(del_list))

    def _scan_expired_files(self, expire_timestamp: float) -> List[Path]:
        if not self.log_path.exists():
            return []

        del_list: List[Path] = []
        self.executor_logger.debug("Start scan expired files")
        for sub_path in (i for i in self.log_path.glob(LOG_NAME_REGEX) if i.is_file()):
            try:
                ctime = os.path.getctime(sub_path.absolute())
                if ctime < expire_timestamp:
                    del_list.append(sub_path)
            except (OSError, FileNotFoundError) as e:
                # 文件可能在扫描过程中被删除
                self.executor_logger.debug(f"Skip file {sub_path}: {e}")
                continue
        return del_list

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

    def after_running(self, logger: logging.Logger) -> None:
        # !!! StreamHandler remove会有问题，需要判断是否是FileHandler
        file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
        for fh in file_handlers:
            logger.debug("close file log object: {}.".format(fh))
            fh.close()
            logger.removeHandler(fh)
