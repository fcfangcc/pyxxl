import logging
from typing import TYPE_CHECKING, Any, Optional, Union

from pyxxl.ctx import g
from pyxxl.log import executor_logger
from pyxxl.types import LogRequest, LogResponse
from pyxxl.utils import try_import

from .common import MAX_LOG_TAIL_LINES, TASK_FORMATTER, LogBase, PyxxlStreamHandler

if TYPE_CHECKING:
    from logging import Handler

    import redis
else:
    redis = try_import("redis")


KEY_PREFIX = "pyxxl:log"


class RedisHandler(logging.Handler):
    terminator = "\n"

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
            xxl_kwargs = g.try_get_run_data()
            record.logId = xxl_kwargs.logId if xxl_kwargs else "NotInTask"
            p = self.rclient.pipeline()
            p.rpush(self.key, self.format(record) + self.terminator)
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
        expired_days: float = 14,
        logger: Optional[logging.Logger] = None,
        prefix: str = KEY_PREFIX,
    ) -> None:
        if redis is None:
            raise ImportError("Depend on redis. pip install redis or pip install pyxxl[redis].")  # pragma: no cover
        self.executor_logger = logger or executor_logger
        self.app = app
        self.log_tail_lines = log_tail_lines or MAX_LOG_TAIL_LINES
        self.expired_seconds = round(expired_days * 3600 * 24)
        if isinstance(redis_client, str):
            rclient = redis.Redis.from_url(redis_client)
        elif isinstance(redis_client, redis.ConnectionPool):
            rclient = redis.Redis(connection_pool=redis_client)
        else:
            raise TypeError(
                "pool expect Union[str, redis.ConnectionPool], got %s." % type(redis_client)
            )  # pragma: no cover
        self.rclient = rclient
        self.prefix = prefix

    def get_logger(
        self,
        log_id: int,
        *,
        stdout: bool = True,
        level: int = logging.INFO,
        expired_seconds: Optional[int] = None,
    ) -> logging.Logger:
        logger = logging.getLogger("pyxxl.task_log.redis.task-{%s}" % log_id)
        logger.propagate = False
        logger.setLevel(level)
        handlers: list[Handler] = [PyxxlStreamHandler()] if stdout else []
        handlers.append(RedisHandler(self.key(log_id), expired_seconds or self.expired_seconds, self.rclient))
        for h in handlers:
            h.setFormatter(TASK_FORMATTER)
            h.setLevel(level)
            logger.addHandler(h)
        return logger

    def key(self, log_id: int) -> str:
        return f"{self.prefix}:{self.app}:{log_id}"

    async def read_task_logs(self, log_id: int) -> str | None:
        key = self.key(log_id)
        if not self.rclient.exists(key):
            return None

        return "".join(i.decode() for i in self.rclient.lrange(key, 0, -1))

    async def get_logs(self, request: LogRequest) -> LogResponse:
        key = self.key(request["logId"])
        from_line = request["fromLineNum"] - 1
        to_line = request["fromLineNum"] - 1 + self.log_tail_lines
        llen = self.rclient.llen(key)
        if from_line >= llen:
            logs = self.NOT_FOUND_LOGS if llen == 0 else ""
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
