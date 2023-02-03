import time

from pathlib import Path

import aiofiles
import pytest

from pyxxl.logger import FileLog, LogRequest, LogResponse


@pytest.mark.asyncio
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
async def test_read_file(req, resp):
    data = "".join(str(i) + "\n" for i in range(1, 80 + 1))
    async with aiofiles.tempfile.NamedTemporaryFile() as f:
        await f.write(data.encode())
        await f.flush()
        await f.seek(0)

        filename = f.name
        handler = FileLog("", log_tail_lines=20)
        assert await handler.get_logs(req, filename=filename) == resp


@pytest.mark.asyncio
async def test_task_logger():
    log_id = int(time.time())
    async with aiofiles.tempfile.TemporaryDirectory() as d:
        handler = FileLog(log_path=d)
        logger = handler.get_logger(log_id, stdout=False)
        logger.error("test error.")
        logger.warning("test warning.")
        logger.handlers.clear()

        read_data = await handler.read_all(log_id)
        for b in ["test error", "test warning", "ERROR", "WARNING"]:
            assert b in read_data


@pytest.mark.asyncio
async def test_task_expired():
    log_id = int(time.time())
    async with aiofiles.tempfile.TemporaryDirectory() as d:
        file_log = FileLog(log_path=d, expired_days=0)
        logger = file_log.get_logger(log_id, stdout=False)
        logger.error("test error.")
        logger.warning("test warning.")
        logger.handlers.clear()

        log_file = Path(file_log.filename(log_id))
        assert log_file.exists()
        await file_log.expired_once()
        assert log_file.exists() is False
