import logging
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, Optional, Union

from pyxxl.types import LogRequest, LogResponse
from pyxxl.utils import STD_FORMATTER, try_import

from .common import MAX_LOG_TAIL_LINES, LogBase

if TYPE_CHECKING:
    from logging import Handler

    import redis
else:
    redis = try_import("redis")


KEY_PREFIX = "pyxxl:log:{app}:{log_id}"


class RedisHandler(logging.Handler):
    def __init__(
        self,
        key: str,
        ttl: int,
        rclient: "redis.Redis",
        *,
        level: int = logging.NOTSET,
        max_lines: Optional[int] = None,
    ) -> None:
        super().__init__(level)
        self.rclient = rclient
        self.key = key
        self.ttl = ttl
        self.max_lines = max_lines or MAX_LOG_TAIL_LINES

    def emit(self, record: Any) -> None:
        try:
            p = self.rclient.pipeline()
            p.rpush(self.key, self.format(record))
            p.ltrim(self.key, -self.max_lines, -1)
            p.expire(self.key, self.ttl)
            p.execute()
        except redis.RedisError as e:  # pragma: no cover
            print("log to redis failed. %s" % str(e))  # pragma: no cover


class RedisLog(LogBase):
    def __init__(
        self,
        app: str,
        redis_client: Union[str, "redis.ConnectionPool"],
        log_tail_lines: int = 0,
        expired_days: int = 14,
    ) -> None:
        if redis is None:
            raise ImportError("Depend on redis. pip install redis or pip install pyxxl[redis].")  # pragma: no cover

        self.app = app
        self.log_tail_lines = log_tail_lines or MAX_LOG_TAIL_LINES
        self.expired_days = expired_days
        if isinstance(redis_client, str):
            rclient = redis.Redis.from_url(redis_client)
        elif isinstance(redis_client, redis.ConnectionPool):
            rclient = redis.Redis(connection_pool=redis_client)
        else:
            raise TypeError(
                "pool expect Union[str, redis.ConnectionPool], got %s." % type(redis_client)
            )  # pragma: no cover
        self.rclient = rclient

    def get_logger(self, log_id: int, *, stdout: bool = True, level: int = logging.INFO) -> logging.Logger:
        logger = logging.getLogger("pyxxl-task-{%s}" % log_id)
        logger.propagate = False
        logger.setLevel(level)
        handlers: list[Handler] = [logging.StreamHandler()] if stdout else []
        handlers.append(RedisHandler(self.key(log_id), self.expired_days * 3600 * 24, self.rclient))
        for h in handlers:
            h.setFormatter(STD_FORMATTER)
            h.setLevel(level)
            logger.addHandler(h)
        return logger

    def key(self, log_id: int) -> str:
        return KEY_PREFIX.format(app=self.app, log_id=log_id)

    async def read_task_logs(self, log_id: int, *, key: str = None) -> str:
        key = key or self.key(log_id)
        # todo: use async
        return "".join(i.decode() for i in self.rclient.lrange(key, 0, -1))

    async def get_logs(self, request: LogRequest, *, key: str = None) -> LogResponse:
        key = key or self.key(request["logId"])
        from_line = request["fromLineNum"] - 1
        to_line = request["fromLineNum"] - 1 + self.log_tail_lines
        llen = self.rclient.llen(key)
        if from_line >= llen:
            logs = "No such logid logs." if llen == 0 else ""
            to_line_num = request["fromLineNum"]
        else:
            # lrange 0 20   [0, 20]
            logs = "".join(i.decode() for i in self.rclient.lrange(key, from_line, to_line - 1))
            to_line_num = to_line

        return LogResponse(
            fromLineNum=request["fromLineNum"],
            toLineNum=to_line_num,
            logContent=logs,
            isEnd=llen <= to_line,
        )

    @asynccontextmanager
    async def mock_write(self, *lines: Any) -> AsyncGenerator[str, None]:
        key = self.key(int(time.time() * 1000))
        self.rclient.rpush(key, *lines)
        yield key
        self.rclient.delete(key)

    @asynccontextmanager
    async def mock_logger(self, log_id: int) -> AsyncGenerator[LogBase, None]:
        yield self
        self.rclient.delete(self.key(log_id))
