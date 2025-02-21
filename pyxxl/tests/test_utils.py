import uuid
from pathlib import Path

from pyxxl.utils import setup_logging, try_import


def test_import():
    assert try_import("pandas") is None
    assert try_import("aiohttp")


def test_logging():
    log_path = "logs/test.log"
    Path(log_path).unlink(missing_ok=True)
    test_log_record = uuid.uuid4().hex
    logger = setup_logging(log_path, "pyxxl-pytest")
    logger.info(test_log_record)
    with open(log_path) as f:
        data = f.readline()
        assert test_log_record in data
        assert "INFO" in data
