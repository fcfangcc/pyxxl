import asyncio
import time

from pyxxl import ExecutorConfig, PyxxlRunner
from pyxxl.ctx import g

# 如果xxl-admin可以直连executor的ip，可以不填写executor_listen_host
config = ExecutorConfig(
    xxl_admin_baseurl="http://localhost:8080/xxl-job-admin/api/",
    executor_app_name="xxl-job-executor-sample",
    executor_url="http://127.0.0.1:9999",  # xxl-admin访问executor的地址
    executor_listen_host="0.0.0.0",  # xxl-admin监听时绑定的host
    debug=True,
)

app = PyxxlRunner(config)


@app.register(name="demoJobHandler")
async def test_task():
    # you can get task params with "g"
    g.logger.info("get executor params: %s" % g.xxl_run_data.executorParams)
    for i in range(10):
        g.logger.warning("test logger %s" % i)
    await asyncio.sleep(5)
    return "成功..."


@app.register(name="xxxxx")
async def test_task3():
    await asyncio.sleep(3)
    return "成功3"


@app.register(name="sync_func")
def test_task4():
    # 如果要在xxl-admin上看到执行日志，打印日志的时候务必用g.logger来打印，默认只打印info及以上的日志
    n = 1
    g.logger.info("Job %s get executor params: %s" % (g.xxl_run_data.jobId, g.xxl_run_data.executorParams))
    # 如果同步任务里面有循环，为了支持cancel操作，必须每次都判断g.cancel_event.
    while n <= 10 and not g.cancel_event.is_set():
        # 如果不需要从xxl-admin中查看日志，可以用自己的logger
        g.logger.info(
            "log to {} logger test_task4.{},params:{}".format(
                g.xxl_run_data.jobId,
                n,
                g.xxl_run_data.executorParams,
            )
        )
        time.sleep(2)
        n += 1
    return "成功3"


if __name__ == "__main__":
    app.run_executor()
