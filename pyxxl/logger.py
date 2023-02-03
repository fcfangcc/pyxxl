import logging

from dataclasses import dataclass
from logging import FileHandler
from pathlib import Path
from typing import TYPE_CHECKING

import aiofiles

from pyxxl.types import LogRequest, LogResponse
from pyxxl.utils import STD_FORMATTER


if TYPE_CHECKING:
    from logging import Handler


LOG_NAME_PREFIX = "pyxxl-{log_id}.log"
MAX_LOG_TAIL_LINES = 1000
logger = logging.getLogger(__name__)


@dataclass
class FileLog:
    log_path: str
    log_tail_lines: int = 0

    def __post_init__(self) -> None:
        if not Path(self.log_path).exists():
            Path(self.log_path).mkdir()  # pragma: no cover
            logger.info("create logdir %s" % self.log_path)  # pragma: no cover
        self.log_tail_lines = self.log_tail_lines or MAX_LOG_TAIL_LINES

    def _filename(self, log_id: int) -> str:
        return Path(self.log_path).joinpath(LOG_NAME_PREFIX.format(log_id=log_id)).absolute().as_posix()

    def get_logger(self, log_id: int, *, stdout: bool = True, level: int = logging.INFO) -> logging.Logger:
        logger = logging.getLogger("pyxxl-task-{%s}" % log_id)
        logger.setLevel(level)
        handlers: list[Handler] = [logging.StreamHandler()] if stdout else []
        handlers.append(FileHandler(self._filename(log_id), delay=True))
        for h in handlers:
            h.setFormatter(STD_FORMATTER)
            h.setLevel(level)
            logger.addHandler(h)
        return logger

    async def get_logs(self, request: LogRequest, *, filename: str = None) -> LogResponse:
        # todo: 优化获取中间行的逻辑，缓存之前每行日志的大小然后直接seek
        logs = ""
        to_line_num = request["fromLineNum"]  # start with 1
        is_end = False
        filename = filename or self._filename(request["logId"])
        try:
            async with aiofiles.open(filename, mode="r") as f:
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
            logs = "No such log file or directory."

        return LogResponse(
            fromLineNum=request["fromLineNum"],
            toLineNum=to_line_num,
            logContent=logs,
            isEnd=is_end,
        )

    async def read_all(self, log_id: int, *, filename: str = None) -> str:
        filename = filename or self._filename(log_id)
        async with aiofiles.open(filename, mode="r") as f:
            return await f.read()

    async def expired_logs(self) -> None:
        # todo
        ...
