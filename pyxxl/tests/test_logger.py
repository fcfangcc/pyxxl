from typing import TYPE_CHECKING, Callable

import aiofiles
import pytest

from pyxxl.logger import DiskLog, LogBase, RedisLog, SQLiteLog
from pyxxl.tests.utils import INSTALL_REDIS, REDIS_TEST_URI
from pyxxl.types import LogRequest, LogResponse
from pyxxl.utils import try_import

from .utils import mock_run_data

if TYPE_CHECKING:
    import redis
else:
    redis = try_import("redis")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "get_log",
    [
        lambda temp_dir: DiskLog(temp_dir, log_tail_lines=20),
        lambda temp_dir: SQLiteLog(temp_dir, log_tail_lines=20),
        pytest.param(
            lambda temp_dir: RedisLog(
                "pyxxl-test", REDIS_TEST_URI, log_tail_lines=20, prefix=str(hash(temp_dir)), expired_days=1
            ),
            marks=pytest.mark.skipif(not INSTALL_REDIS, reason="no redis package."),
        ),
    ],
    ids=["disk", "sqlite", "redis"],
)
class TestTaskLogger:
    @pytest.mark.parametrize(
        "req,resp",
        [
            (
                LogRequest(logDateTim=0, logId=0, fromLineNum=1),
                LogResponse(fromLineNum=1, toLineNum=20, logContent="", isEnd=False),
            ),
            (
                LogRequest(logDateTim=0, logId=0, fromLineNum=21),
                LogResponse(fromLineNum=21, toLineNum=40, logContent="", isEnd=False),
            ),
            (
                LogRequest(logDateTim=0, logId=0, fromLineNum=81),
                LogResponse(fromLineNum=81, toLineNum=81, logContent="", isEnd=True),
            ),
        ],
    )
    async def test_read_storage(
        self,
        get_log: Callable[[str], LogBase],
        req: LogRequest,
        resp: LogResponse,
        job_id: int,
        log_id: int,
    ):
        async with mock_run_data(job_id, log_id), aiofiles.tempfile.TemporaryDirectory() as temp_dir:
            log = get_log(temp_dir)
            logger = log.get_logger(job_id, log_id, stdout=False)
            for i in range(1, 81):
                logger.info(str(i))

            req["jobId"] = job_id
            req["logId"] = log_id
            log_resp = await log.get_logs(req)
            assert log_resp["fromLineNum"] == resp["fromLineNum"]
            assert log_resp["toLineNum"] == resp["toLineNum"]
            assert log_resp["isEnd"] == resp["isEnd"]
            # assert await log.get_logs(req) == resp

    async def test_logger(self, get_log: Callable[[str], LogBase], job_id: int, log_id: int):
        async with mock_run_data(job_id, log_id), aiofiles.tempfile.TemporaryDirectory() as temp_dir:
            log = get_log(temp_dir)
            logger = log.get_logger(job_id, log_id, stdout=False)
            try:
                raise ValueError("test error")
            except ValueError as e:
                logger.error(e, exc_info=True)
            logger.warning("test warning.")
            logger.handlers.clear()

            read_data = await log.read_task_logs(job_id, log_id)
            assert read_data
            for b in ["test error", "test warning", "ERROR", "WARNING"]:
                assert b in read_data

            log_resp = await log.get_logs(LogRequest(logDateTim=0, logId=-1, fromLineNum=81, jobId=job_id))
            assert "No such logid logs" in log_resp["logContent"]

    async def test_disk_expired(self, get_log: Callable[[str], LogBase], job_id: int, log_id: int):
        async with mock_run_data(job_id, log_id), aiofiles.tempfile.TemporaryDirectory() as temp_dir:
            log = get_log(temp_dir)
            logger = log.get_logger(job_id, log_id, stdout=False)
            logger.error("test error.")
            logger.warning("test warning.")
            logger.handlers.clear()

            logs = await log.read_task_logs(job_id, log_id)
            assert logs is not None

            expired = await log.expired_once(expired_seconds=0)
            if not expired:
                pytest.skip("expired_once not implemented in this logger type")
            logs = await log.read_task_logs(job_id, log_id)
            assert logs is None
