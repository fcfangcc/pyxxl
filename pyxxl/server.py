import logging
from typing import TYPE_CHECKING

from aiohttp import web

from pyxxl import error
from pyxxl.schema import RunData
from pyxxl.utils import try_import

if TYPE_CHECKING:
    from pyxxl.logger import LogBase

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()


@routes.post("/beat")
async def beat(request: web.Request) -> web.Response:
    logger.debug("beat")
    return web.json_response(dict(code=200, msg=None))


@routes.post("/idleBeat")
async def idle_beat(request: web.Request) -> web.Response:
    data = await request.json()
    job_id = data["jobId"]
    logger.debug("idleBeat: %s" % data)
    if await request.app["executor"].is_running(data["jobId"]):
        return web.json_response(dict(code=500, msg="job %s is running." % job_id))
    return web.json_response(dict(code=200, msg=None))


@routes.post("/run")
async def run(request: web.Request) -> web.Response:
    """
        {
        "jobId":1,                                  // 任务ID
        "executorHandler":"demoJobHandler",         // 任务标识
        "executorParams":"demoJobHandler",          // 任务参数
        "executorBlockStrategy":"COVER_EARLY",      // 任务阻塞策略，可选值参考 com.xxl.job.core.enums.ExecutorBlockStrategyEnum
        "executorTimeout":0,                        // 任务超时时间，单位秒，大于零时生效
        "logId":1,                                  // 本次调度日志ID
        "logDateTime":1586629003729,                // 本次调度日志时间
        "glueType":"BEAN",                          // 任务模式，可选值参考 com.xxl.job.core.glue.GlueTypeEnum
        "glueSource":"xxx",                         // GLUE脚本代码
        "glueUpdatetime":1586629003727,             // GLUE脚本更新时间，用于判定脚本是否变更以及是否需要刷新
        "broadcastIndex":0,                         // 分片参数：当前分片
        "broadcastTotal":0                          // 分片参数：总分片
    }
    """
    data = await request.json()
    run_data = RunData(**data)
    logger.info("Get task request. jobId=%s logId=%s [%s]" % (run_data.jobId, run_data.logId, run_data))
    try:
        await request.app["executor"].run_job(run_data)
    except error.JobDuplicateError as e:
        return web.json_response(dict(code=500, msg=e.message))
    except error.JobNotFoundError as e:
        return web.json_response(dict(code=500, msg=e.message))

    return web.json_response(dict(code=200, msg=None))


@routes.post("/kill")
async def kill(request: web.Request) -> web.Response:
    data = await request.json()
    await request.app["executor"].cancel_job(data["jobId"])
    return web.json_response(dict(code=200, msg=None))


@routes.post("/log")
async def log(request: web.Request) -> web.Response:
    """
        {
        "logDateTim":0,     // 本次调度日志时间
        "logId":0,          // 本次调度日志ID
        "fromLineNum":0     // 日志开始行号，滚动加载日志
    }
    """
    data = await request.json()
    logger.debug("get log request %s" % data)
    executor_log: LogBase = request.app["executor_log"]
    response = {
        "code": 200,
        "msg": None,
        "content": await executor_log.get_logs(data),
    }
    return web.json_response(response)


def create_app() -> web.Application:
    """
    xxl_admin_baseurl xxl调度中心的接口地址，如http://localhost:8080/xxl-job-admin/api/
    executor_names 执行器名称列表
    executor_baseurl 执行器http接口的地址 如http://172.17.0.1:9999
    """
    app = web.Application()
    app.add_routes(routes)
    if try_import("prometheus_client"):
        from pyxxl.prometheus import mount_app

        mount_app(app)

    return app
