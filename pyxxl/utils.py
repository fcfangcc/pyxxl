import logging
import socket

from typing import Any, List, Optional

from pyxxl.ctx import g


def get_network_ip() -> str:
    """获取本机地址,会获取首个网络地址"""
    _, _, ipaddrlist = socket.gethostbyname_ex(socket.gethostname())
    return ipaddrlist[0]


def _init_log_record_factory() -> None:
    old_factory = logging.getLogRecordFactory()

    def _record_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record: Any = old_factory(*args, **kwargs)
        xxl_kwargs = g.try_get("xxl_kwargs")
        record.logId = xxl_kwargs.logId if xxl_kwargs else "NotInTask"
        return record

    logging.setLogRecordFactory(_record_factory)


def setup_logging(
    level: int = logging.INFO,
    custom_handlers: Optional[List[logging.Handler]] = None,
    std_formatter: Optional[logging.Formatter] = None,
) -> logging.Logger:
    if std_formatter is None:
        DEFAULT_FORMAT = (
            "%(asctime)s.%(msecs)03d [%(threadName)s] [%(logId)s] "
            "%(levelname)s %(pathname)s(%(funcName)s:%(lineno)d) - %(message)s"
        )
        DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
        std_formatter = logging.Formatter(DEFAULT_FORMAT, datefmt=DATE_FORMAT)

    _init_log_record_factory()
    logger = logging.getLogger("pyxxl")
    logger.setLevel(level)

    handler = logging.StreamHandler()
    handler.setFormatter(std_formatter)
    handler.setLevel(level)
    logger.addHandler(handler)

    if custom_handlers:
        for h in custom_handlers:
            h.setFormatter(std_formatter)
            h.setLevel(level)
            logger.addHandler(h)
    return logger
