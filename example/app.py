import asyncio
import logging
import time

from pyxxl import PyxxlRunner
from pyxxl.ctx import g
from pyxxl.utils import setup_logging


setup_logging(logging.INFO)

app = PyxxlRunner(
    "http://localhost:8080/xxl-job-admin/api/",
    executor_name="xxl-job-executor-sample",
    port=9999,
    host="172.17.0.1",
)


@app.handler.register(name="demoJobHandler")
async def test_task():
    # you can get task params with "g"
    print("get executor params: %s" % g.xxl_run_data.executorParams)
    await asyncio.sleep(5)
    return "成功..."


@app.handler.register(name="xxxxx")
async def test_task3():
    await asyncio.sleep(3)
    return "成功3"


@app.handler.register(name="sync_func")
def test_task4():
    time.sleep(3)
    return "成功3"


app.run_executor()
