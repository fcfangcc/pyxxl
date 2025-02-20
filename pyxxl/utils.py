import importlib
import logging
import platform
import socket
from logging.handlers import RotatingFileHandler
from typing import Any, List, Optional

DEFAULT_FORMAT = (
    "%(asctime)s.%(msecs)03d [%(threadName)s] %(levelname)s %(pathname)s(%(funcName)s:%(lineno)d) - %(message)s"
)
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
STD_FORMATTER = logging.Formatter(DEFAULT_FORMAT, datefmt=DATE_FORMAT)
DEFAULT_FILE_SIZE = 50 * 1024 * 1024
DEFAULT_BACKUP_FILE_COUNT = 5


def get_network_ip() -> str:
    """获取本机地址,会获取首个网络地址"""
    if platform.system() == "Darwin":
        return "127.0.0.1"  # todo
    else:
        _, _, ipaddrlist = socket.gethostbyname_ex(socket.gethostname())
    return ipaddrlist[0]


def setup_logging(
    path: str,
    name: str,
    level: int = logging.INFO,
    custom_handlers: Optional[List[logging.Handler]] = None,
    std_formatter: Optional[logging.Formatter] = None,
) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    std_formatter = std_formatter or STD_FORMATTER

    logger.setLevel(level)
    logger.propagate = False

    handlers: List[logging.Handler] = [
        logging.StreamHandler(),
        RotatingFileHandler(path, maxBytes=DEFAULT_FILE_SIZE, backupCount=DEFAULT_BACKUP_FILE_COUNT, delay=True),
    ]
    if custom_handlers:
        handlers.extend(custom_handlers)

    for h in handlers:
        h.setFormatter(std_formatter)
        h.setLevel(level)
        logger.addHandler(h)
    return logger


def try_import(module: str) -> Optional[Any]:
    try:
        return importlib.import_module(module)
    except ImportError:
        pass
    return None
