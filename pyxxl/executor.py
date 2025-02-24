from __future__ import annotations

import asyncio
import logging
import threading
import time
import warnings
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, MutableSet, Optional

from pyxxl import error
from pyxxl.ctx import g
from pyxxl.enum import executorBlockStrategy
from pyxxl.log import executor_logger
from pyxxl.logger import DiskLog, LogBase, new_logger
from pyxxl.schema import RunData
from pyxxl.setting import ExecutorConfig
from pyxxl.types import DecoratedCallable
from pyxxl.xxl_client import XXL

# https://docs.python.org/3.10/library/asyncio-task.html#asyncio.create_task
_BACKGROUND_TASKS: MutableSet[asyncio.Task] = set()


def spawn_task(task: asyncio.Task) -> None:
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_TASKS.discard)


@dataclass
class HandlerInfo:
    handler: Callable
    is_async: bool = False

    def __str__(self) -> str:
        return "<HandlerInfo {}>".format(self.handler.__name__)

    def __post_init__(self) -> None:
        self.is_async = asyncio.iscoroutinefunction(self.handler)

    async def start(self, timeout: int) -> Any:
        if self.is_async:
            return await asyncio.wait_for(self.handler(), timeout=timeout)
        # https://stackoverflow.com/questions/71416383/python-asyncio-cancelling-a-to-thread-task-wont-stop-the-thread
        # 由于线程无法直接取消，这里发送一个event，供开发者自己接收信号来判断是否需要取消
        event = threading.Event()
        g.set_cancel_event(event)
        try:
            return await asyncio.wait_for(asyncio.to_thread(self.handler), timeout=timeout)
        except (asyncio.exceptions.TimeoutError, asyncio.CancelledError) as e:
            event.set()
            # logger.debug("Get error for sync task {}".format(self))
            raise e


class XXLTask:
    def __init__(self, task: asyncio.Task, data: RunData):
        self.task = task
        self.data = data

    def __str__(self) -> str:
        return "<XXLTask task={} data={}>".format(self.task, self.data)

    @property
    def cancel(self) -> Any:
        return self.task.cancel


class JobHandler:
    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._handlers: Dict[str, HandlerInfo] = {}
        self.logger = logger or executor_logger

    def register(
        self, *args: Any, name: Optional[str] = None, replace: bool = False
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        """将函数注册到可执行的job中,如果其他地方要调用该方法,replace修改为True"""

        def func_wrapper(func: DecoratedCallable) -> DecoratedCallable:
            handler_name = name or func.__name__
            if handler_name in self._handlers and replace is False:
                raise error.JobRegisterError("handler %s already registered." % handler_name)
            handler = HandlerInfo(handler=func)
            if not handler.is_async:
                warnings.warn(
                    "Using the sync method will unknown blocking exception, consider using async method.",
                    SyntaxWarning,
                    stacklevel=2,
                )
            self._handlers[handler_name] = handler
            self.logger.debug("register job %s,is async: %s" % (handler_name, asyncio.iscoroutinefunction(func)))

            return func

        if len(args) == 1:
            return func_wrapper(args[0])

        return func_wrapper

    def get(self, name: str) -> Optional[HandlerInfo]:
        return self._handlers.get(name, None)

    def handlers_info(self) -> List[str]:
        return ["<%s is_async:%s>" % (k, v.is_async) for k, v in self._handlers.items()]


class Executor:
    def __init__(
        self,
        xxl_client: XXL,
        config: ExecutorConfig,
        *,
        handler: Optional[JobHandler] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        logger_factory: Optional[LogBase] = None,
        successed_callback: Optional[Callable] = None,
        failed_callback: Optional[Callable] = None,
    ) -> None:
        """执行器，真正的调度任务和策略都在这里

        Args:
            xxl_client (XXL): xxl客户端
            config (ExecutorConfig): 配置参数
            handler (Optional[JobHandler], optional): Defaults to None.
            loop (Optional[asyncio.AbstractEventLoop], optional): Defaults to None.
        """

        self.xxl_client = xxl_client
        self.config = config

        self.handler: JobHandler = handler or JobHandler()
        self.loop = loop or asyncio.get_event_loop()
        self.tasks: Dict[int, XXLTask] = {}
        self.queue: Dict[int, asyncio.Queue[RunData]] = defaultdict(
            lambda: asyncio.Queue(maxsize=self.config.task_queue_length)
        )
        self.lock = asyncio.Lock()
        self.thread_pool = ThreadPoolExecutor(
            max_workers=self.config.max_workers,
            thread_name_prefix="pyxxl_pool",
        )
        self.logger_factory = logger_factory or DiskLog(self.config.log_local_dir)
        self.successed_callback = successed_callback or (lambda: 1)
        self.failed_callback = failed_callback or (lambda x: 1)
        self.loop.set_default_executor(self.thread_pool)

    @property
    def executor_logger(self) -> logging.Logger:
        return self.config.executor_logger

    async def run_job(self, data: RunData) -> str:
        handler_obj = self.handler.get(data.executorHandler)
        if not handler_obj:
            self.executor_logger.warning("handler %s not found." % data.executorHandler)
            raise error.JobNotFoundError("handler %s not found." % data.executorHandler)

        # 一个执行器同时只能执行一个jobId相同的任务
        async with self.lock:
            current_task = self.tasks.get(data.jobId)
            if not current_task and self.get_queue(data.jobId).empty():
                self.tasks[data.jobId] = XXLTask(self.loop.create_task(self._run(data)), data)
                return "Running"

            self.executor_logger.warning("jobId {} is running. current_task={}".format(data.jobId, current_task))
            # pylint: disable=no-else-raise
            if data.executorBlockStrategy == executorBlockStrategy.DISCARD_LATER.value:
                raise error.JobDuplicateError(
                    "The same job [%s] is already executing and this has been discarded." % data
                )
            elif data.executorBlockStrategy == executorBlockStrategy.COVER_EARLY.value:
                msg = "Job {} BlockStrategy is COVER_EARLY, logId {} replaced.".format(data.jobId, data.logId)
                self.executor_logger.warning(msg)
                spawn_task(self.loop.create_task(self.cancel_job(data.jobId, include_queue=False)))
                await self.get_queue(data.jobId).put(data)
                return msg
            elif data.executorBlockStrategy == executorBlockStrategy.SERIAL_EXECUTION.value:
                queue = self.get_queue(data.jobId)
                if queue.full():
                    msg = "Job {job_id} is  SERIAL, queue length more than {maxsize}." "Job {job}  discard!".format(
                        job_id=data.jobId, job=data, maxsize=queue.maxsize
                    )
                    self.executor_logger.error(msg)
                    raise error.JobDuplicateError(msg)
                else:
                    msg = "job {job_id} is in queen, logId {log_id} ranked {ranked}th [max={maxsize}]...".format(
                        job_id=data.jobId, log_id=data.logId, ranked=queue.qsize() + 1, maxsize=queue.maxsize
                    )
                    self.executor_logger.info(msg)
                    await queue.put(data)
                    return msg
            else:
                raise error.JobParamsError(
                    "unknown executorBlockStrategy [%s]." % data.executorBlockStrategy,
                    executorBlockStrategy=data.executorBlockStrategy,
                )

    async def cancel_job(self, job_id: int, include_queue: bool = True) -> None:
        self.executor_logger.warning("start kill job: job_id={}".format(job_id))
        await asyncio.sleep(0.01)  # sleep for pytest

        async with self.lock:
            if include_queue:
                queue = self.get_queue(job_id)
                while not queue.empty():
                    data = queue.get_nowait()
                    self.executor_logger.warning("Discard jobId {} from queue,data: {}".format(job_id, data))

            task = self.tasks.get(job_id, None)
            if task:
                # https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.cancel
                task.cancel()
                try:
                    await task.task
                except asyncio.CancelledError:
                    self.executor_logger.warning("Job %s cancelled." % job_id)

    async def is_running(self, job_id: int) -> bool:
        return job_id in self.tasks

    async def _run(self, data: RunData) -> None:
        handler = self.handler.get(data.executorHandler)
        assert handler
        g.set_xxl_run_data(data)
        with new_logger(self.logger_factory, data.logId) as task_logger:
            start_time = int(time.time() * 1000)
            try:
                task_logger.info("Start job jobId=%s logId=%s [%s]" % (data.jobId, data.logId, data))
                timeout = data.executorTimeout or self.config.task_timeout
                result = await handler.start(timeout)
                task_logger.info("Job finished jobId=%s logId=%s" % (data.jobId, data.logId))
                await self.xxl_client.callback(data.logId, start_time, code=200, msg=result)
                self.successed_callback()
            except asyncio.CancelledError as e:
                task_logger.info(e, exc_info=True)
                await self.xxl_client.callback(data.logId, start_time, code=500, msg="CancelledError")
                self.failed_callback("cancelled")
            except asyncio.exceptions.TimeoutError as e:
                # 同步任务run_in_executor超时会抛出TimeoutError异常
                # !!! 但是注意线程里面的任务仍然在运行，可能会占满所有的线程池
                task_logger.warning(e, exc_info=True)
                await self.xxl_client.callback(data.logId, start_time, code=500, msg="TimeoutError")
                self.failed_callback("timeout")
            except Exception as err:  # pylint: disable=broad-except
                task_logger.exception(err, exc_info=True)
                await self.xxl_client.callback(data.logId, start_time, code=500, msg=str(err))
                self.failed_callback("exception")
            finally:
                if self.lock.locked():
                    await self._finish(data.jobId)
                else:
                    async with self.lock:
                        await self._finish(data.jobId)

    async def _finish(self, job_id: int) -> None:
        # 所有移除tasks的操作全部在这里执行
        finish_task = self.tasks.pop(job_id, None)
        self.executor_logger.info("Finish task {}".format(finish_task))
        queue = self.get_queue(job_id)
        if not queue.empty():
            data = queue.get_nowait()
            self.executor_logger.info(
                "Get data from queue jobId={}, after queueSize={}, data={}".format(job_id, queue.qsize(), data)
            )
            self.tasks[job_id] = XXLTask(self.loop.create_task(self._run(data)), data)
            queue.task_done()

    async def shutdown(self) -> None:
        await asyncio.sleep(0.01)  # sleep for pytest

        async with self.lock:
            self.queue.clear()
            for _, task in self.tasks.items():
                task.task.cancel()

    async def graceful_close(self, timeout: int = 60) -> None:
        """优雅关闭"""
        await asyncio.sleep(0.01)  # sleep for pytest

        async def _graceful_close() -> None:
            while len(self.tasks) > 0 or any(i.qsize() > 0 for i in self.queue.values()):
                await asyncio.wait([i.task for i in self.tasks.values()])
                await asyncio.sleep(0.05)

        await asyncio.wait_for(_graceful_close(), timeout=timeout)

    def reset_handler(self, handler: Optional[JobHandler] = None) -> None:
        self.handler = handler or JobHandler()

    def get_queue(self, job_id: int) -> asyncio.Queue[RunData]:
        return self.queue[job_id]
