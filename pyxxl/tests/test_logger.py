import time
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import aiofiles
import pytest

from pyxxl.logger import DiskLog, LogBase, RedisLog
from pyxxl.tests.utils import INSTALL_REDIS, REDIS_TEST_URI
from pyxxl.types import LogRequest, LogResponse
from pyxxl.utils import try_import

if TYPE_CHECKING:
    import redis
else:
    redis = try_import("redis")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "get_log",
    [
        lambda: DiskLog("", log_tail_lines=20),
        pytest.param(
            lambda: RedisLog("pyxxl-test", REDIS_TEST_URI, log_tail_lines=20),
            marks=pytest.mark.skipif(not INSTALL_REDIS, reason="no redis package."),
        ),
    ],
)
@pytest.mark.parametrize(
    "req,resp",
    [
        (
            LogRequest(logDateTim=0, logId=0, fromLineNum=1),
            LogResponse(
                fromLineNum=1,
                toLineNum=20,
                logContent="".join(str(i) + "\n" for i in range(1, 21)),
                isEnd=False,
            ),
        ),
        (
            LogRequest(logDateTim=0, logId=0, fromLineNum=21),
            LogResponse(
                fromLineNum=21,
                toLineNum=40,
                logContent="".join(str(i) + "\n" for i in range(21, 41)),
                isEnd=False,
            ),
        ),
        (
            LogRequest(logDateTim=0, logId=0, fromLineNum=81),
            LogResponse(
                fromLineNum=81,
                toLineNum=81,
                logContent="",
                isEnd=True,
            ),
        ),
    ],
)
async def test_read_file(get_log: Callable[..., LogBase], req, resp):
    log = get_log()
    data = [str(i).encode() + b"\n" for i in range(1, 80 + 1)]
    async with log.mock_write(*data) as key:
        assert await log.get_logs(req, key=key) == resp


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "get_log",
    [
        lambda: DiskLog("", log_tail_lines=20),
        pytest.param(
            lambda: RedisLog("pyxxl-test", REDIS_TEST_URI, log_tail_lines=20),
            marks=pytest.mark.skipif(not INSTALL_REDIS, reason="no redis package."),
        ),
        pytest.param(
            lambda: RedisLog("pyxxl-test", redis.ConnectionPool.from_url(REDIS_TEST_URI), log_tail_lines=20),
            marks=pytest.mark.skipif(not INSTALL_REDIS, reason="no redis package."),
        ),
    ],
    ids=["disk", "redis", "redis-pool"],
)
async def test_disk_logger(get_log: Callable[..., LogBase]):
    log = get_log()
    log_id = int(time.time())
    async with log.mock_logger(log_id) as mock_log:
        logger = mock_log.get_logger(log_id)
        logger.error("test error.")
        logger.warning("test warning.")
        logger.handlers.clear()

        read_data = await mock_log.read_task_logs(log_id)
        for b in ["test error", "test warning", "ERROR", "WARNING"]:
            assert b in read_data
    # test file not found
    assert (
        "No such logid logs"
        in (
            await log.get_logs(
                LogRequest(logDateTim=0, logId=0, fromLineNum=81),
                key="xxxxxxxxxxxx.log",
            )
        )["logContent"]
    )


@pytest.mark.asyncio
async def test_disk_expired():
    log_id = int(time.time())
    async with aiofiles.tempfile.TemporaryDirectory() as d:
        file_log = DiskLog(log_path=d, expired_days=0)
        logger = file_log.get_logger(log_id, stdout=False)
        logger.error("test error.")
        logger.warning("test warning.")
        logger.handlers.clear()

        log_file = Path(file_log.key(log_id))
        assert log_file.exists()
        await file_log.expired_once()
        assert log_file.exists() is False
