import asyncio
import time

from pyxxl import ExecutorConfig, PyxxlRunner
from pyxxl.ctx import g

config = ExecutorConfig(
    xxl_admin_baseurl="http://localhost:8080/xxl-job-admin/api/",
    executor_app_name="xxl-job-executor-sample",
    executor_host="172.17.0.1",
)

app = PyxxlRunner(config)


@app.handler.register(name="demoJobHandler")
async def test_task():
    # you can get task params with "g"
    g.logger.info("get executor params: %s" % g.xxl_run_data.executorParams)
    for i in range(10):
        g.logger.warning("test logger %s" % i)
    await asyncio.sleep(5)
    return "成功..."


@app.handler.register(name="xxxxx")
async def test_task3():
    await asyncio.sleep(3)
    return "成功3"


@app.handler.register(name="sync_func")
def test_task4():
    # 如果要在xxl-admin上看到执行日志，打印日志的时候务必用g.logger来打印，默认只打印info及以上的日志
    n = 1
    g.logger.info("get executor params: %s" % g.xxl_run_data.executorParams)
    # 如果同步任务里面有循环，为了支持cancel操作，必须每次都判断g.cancel_event.
    while not g.cancel_event.is_set() and n <= 10:
        g.logger.info("log to logger test_task4.{}".format(n))
        time.sleep(3)
        n += 1
    return "成功3"


app.run_executor()
