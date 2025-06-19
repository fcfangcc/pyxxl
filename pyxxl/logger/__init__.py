import logging
from contextlib import contextmanager
from typing import Generator, Optional

from pyxxl.ctx import g
from pyxxl.setting import ExecutorConfig

from .common import LogBase
from .disk import DiskLog
from .redis import RedisLog
from .sqlite import SQLiteLog

__all__ = ["DiskLog", "RedisLog", "SQLiteLog", "new_logger", "init_task_log"]


@contextmanager
def new_logger(
    factory: LogBase, job_id: int, log_id: int, *, expired_seconds: Optional[int] = None
) -> Generator[logging.Logger, None, None]:
    logger = factory.get_logger(job_id, log_id, expired_seconds=expired_seconds)
    token = g.set_task_logger(logger)
    yield logger
    factory.after_running(logger)
    g._LOGGER.reset(token)


def init_task_log(config: ExecutorConfig) -> LogBase:
    if config.log_target == "disk":
        return DiskLog(
            log_path=config.log_local_dir,
            expired_days=config.log_expired_days,
            logger=config.executor_logger,
        )

    elif config.log_target == "redis":
        return RedisLog(
            config.executor_app_name,
            config.log_redis_uri,
            expired_days=config.log_expired_days,
            logger=config.executor_logger,
        )

    elif config.log_target == "sqlite":
        return SQLiteLog(
            log_path=config.log_local_dir,
            expired_days=config.log_expired_days,
            logger=config.executor_logger,
        )
    else:
        raise NotImplementedError(f"Unsupported log target: {config.log_target}")
