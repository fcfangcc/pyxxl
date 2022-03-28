import logging
import asyncio

from pyxxl import PyxxlRunner, job_hander

logger = logging.getLogger("pyxxl")
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


@job_hander
async def test_task():
    await asyncio.sleep(30)
    return "成功30"


@job_hander(name="xxxxx")
async def test_task3():
    await asyncio.sleep(3)
    return "成功3"


runner = PyxxlRunner(
    "http://localhost:8080/xxl-job-admin/api/",
    executor_name="pyxxl",
    port=9999,
    host="172.17.0.1",
)
runner.run_executor()