from dataclasses import asdict, is_dataclass
from typing import Any

from aiohttp import web
from prometheus_client import Counter, Gauge, Info
from prometheus_client.exposition import _bake_output
from prometheus_client.registry import REGISTRY

from pyxxl.ctx import g
from pyxxl.executor import Executor

FAILED_COUNTER = Counter("failed", "task failed number.", ["jobId", "reason"])
SUCCESS_COUNTER = Counter("success", "task success number.", ["jobId"])
RUNNING_TASKS = Gauge("running_tasks", "running tasks")
QUEUE_TASKS = Gauge("queue_tasks", "queue_tasks", ["jobId"])
RUNNING_TASK_INFO = Info("running_task_info", "running task info", ["pk"])
QUEUE_TASKS_INFO = Info("queue_task_info", "queue task info", ["pk"])

routes = web.RouteTableDef()


def success() -> None:
    SUCCESS_COUNTER.labels(g.xxl_run_data.jobId).inc(1)


def failed(reason: str) -> None:
    FAILED_COUNTER.labels(g.xxl_run_data.jobId, reason).inc(1)


def as_str_dict(obj: Any) -> dict:
    if is_dataclass(obj):
        obj = asdict(obj)
    return {k: str(v) for k, v in obj.items()}


@routes.get("/metrics")
async def metrics(request: web.Request) -> web.Response:
    # init
    RUNNING_TASK_INFO.clear()
    QUEUE_TASKS_INFO.clear()

    # export executor info
    executor: Executor = request.app["executor"]
    RUNNING_TASKS.set(len(executor.tasks))

    for k, v in executor.tasks.items():
        RUNNING_TASK_INFO.labels(k).info(as_str_dict(v.data))

    for kk, vv in executor.queue.items():
        QUEUE_TASKS.labels(kk).set(len(vv))
        info = as_str_dict({str(idx): as_str_dict(vv[idx]) for idx in range(len(vv))})
        QUEUE_TASKS_INFO.labels(kk).info(info)

    params = request.query
    # todo
    _, headers, output = _bake_output(REGISTRY, "", "", params, True)
    return web.Response(body=output, headers=headers)


def mount_app(app: web.Application) -> None:
    app.add_routes(routes)
