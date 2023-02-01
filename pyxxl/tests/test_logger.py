import time

import aiofiles
import pytest

from pyxxl.logger import FileLog, LogRequest, LogResponse


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "req,resp",
    [
        (
            LogRequest(logDateTim=0, logId=0, fromLineNum=0),
            LogResponse(
                fromLineNum=0,
                toLineNum=20,
                logContent="".join(str(i) + "\n" for i in range(0, 20)),
                isEnd=False,
            ),
        ),
        (
            LogRequest(logDateTim=0, logId=0, fromLineNum=20),
            LogResponse(
                fromLineNum=20,
                toLineNum=40,
                logContent="".join(str(i) + "\n" for i in range(20, 40)),
                isEnd=False,
            ),
        ),
        (
            LogRequest(logDateTim=0, logId=0, fromLineNum=80),
            LogResponse(
                fromLineNum=80,
                toLineNum=80,
                logContent="",
                isEnd=True,
            ),
        ),
    ],
)
async def test_read_file(req, resp):
    data = "".join(str(i) + "\n" for i in range(0, 80))
    async with aiofiles.tempfile.NamedTemporaryFile() as f:
        await f.write(data.encode())
        await f.flush()
        await f.seek(0)

        filename = f.name
        handler = FileLog("")
        assert await handler.get_log_lines(req, filename=filename) == resp


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
