import asyncio
import contextvars
import logging
import threading
import time
import warnings
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from pyxxl import error
from pyxxl.ctx import g
from pyxxl.enum import executorBlockStrategy
from pyxxl.logger import DiskLog, LogBase
from pyxxl.schema import RunData
from pyxxl.setting import ExecutorConfig
from pyxxl.types import DecoratedCallable
from pyxxl.xxl_client import XXL

logger = logging.getLogger(__name__)


@dataclass
class HandlerInfo:
    handler: Callable

    def __str__(self) -> str:
        return "<HandlerInfo {}>".format(self.handler.__name__)

    @property
    def is_async(self) -> bool:
        return asyncio.iscoroutinefunction(self.handler)

    async def start_async(self, timeout: int) -> Any:
        assert self.is_async
        return await asyncio.wait_for(self.handler(), timeout=timeout)

    async def start_sync(self, loop: asyncio.AbstractEventLoop, pool: ThreadPoolExecutor, timeout: int) -> Any:
        assert not self.is_async
        event = threading.Event()
        g.set_cancel_event(event)
        context = contextvars.copy_context()
        try:
            return await asyncio.wait_for(loop.run_in_executor(pool, context.run, self.handler), timeout=timeout)
        except (asyncio.exceptions.TimeoutError, asyncio.CancelledError) as e:
            event.set()
            logger.debug("Get error for sync task {}".format(self))
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
    def __init__(self) -> None:
        self._handlers: Dict[str, HandlerInfo] = {}

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
                    2,
                )
            self._handlers[handler_name] = handler
            logger.debug("register job %s,is async: %s" % (handler_name, asyncio.iscoroutinefunction(func)))

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
        successd_callback: Optional[Callable] = None,
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
        self.queue: Dict[int, List[RunData]] = defaultdict(list)
        self.lock = asyncio.Lock()
        self.thread_pool = ThreadPoolExecutor(
            max_workers=self.config.max_workers,
            thread_name_prefix="pyxxl_pool",
        )
        self.logger_factory = logger_factory or DiskLog(self.config.log_local_dir)
        self.successd_callback = successd_callback or (lambda: 1)
        self.failed_callback = failed_callback or (lambda x: 1)

    async def shutdown(self) -> None:
        for _, task in self.tasks.items():
            task.task.cancel()

    async def run_job(self, run_data: RunData) -> None:
        handler_obj = self.handler.get(run_data.executorHandler)
        if not handler_obj:
            logger.warning("handler %s not found." % run_data.executorHandler)
            raise error.JobNotFoundError("handler %s not found." % run_data.executorHandler)

        # 一个执行器同时只能执行一个jobId相同的任务
        async with self.lock:
            current_task = self.tasks.get(run_data.jobId)
            if current_task:
                logger.warning("jobId {} is running. current_task={}".format(run_data.jobId, current_task))
                # pylint: disable=no-else-raise
                if run_data.executorBlockStrategy == executorBlockStrategy.DISCARD_LATER.value:
                    raise error.JobDuplicateError(
                        "The same job [%s] is already executing and this has been discarded." % run_data.jobId
                    )
                elif run_data.executorBlockStrategy == executorBlockStrategy.COVER_EARLY.value:
                    logger.warning("job %s is  COVER_EARLY, logId %s replaced." % (run_data.jobId, run_data.logId))
                    await self._cancel(run_data.jobId)
                elif run_data.executorBlockStrategy == executorBlockStrategy.SERIAL_EXECUTION.value:
                    if len(self.queue[run_data.jobId]) >= self.config.task_queue_length:
                        msg = (
                            "job {job_id} is  SERIAL, queue length more than {max_length}."
                            "logId {log_id}  discard!".format(
                                job_id=run_data.jobId,
                                log_id=run_data.logId,
                                max_length=self.config.task_queue_length,
                            )
                        )
                        logger.error(msg)
                        raise error.JobDuplicateError(msg)
                    else:
                        queue = self.queue[run_data.jobId]
                        logger.info(
                            "job {job_id} is in queen, logId {log_id} ranked {ranked}th [max={max_length}]...".format(
                                job_id=run_data.jobId,
                                log_id=run_data.logId,
                                ranked=len(queue) + 1,
                                max_length=self.config.task_queue_length,
                            )
                        )
                        queue.append(run_data)
                        return
                else:
                    raise error.JobParamsError(
                        "unknown executorBlockStrategy [%s]." % run_data.executorBlockStrategy,
                        executorBlockStrategy=run_data.executorBlockStrategy,
                    )

            start_time = int(time.time() * 1000)
            self.tasks[run_data.jobId] = XXLTask(
                self.loop.create_task(self._run(handler_obj, start_time, run_data)),
                run_data,
            )

    async def cancel_job(self, job_id: int) -> None:
        async with self.lock:
            logger.warning("start kill job: job_id={}".format(job_id))
            await self._cancel(job_id)

    async def is_running(self, job_id: int) -> bool:
        return job_id in self.tasks

    async def _run(self, handler: HandlerInfo, start_time: int, data: RunData) -> None:
        g.set_xxl_run_data(data)
        g.set_task_logger(self.logger_factory.get_logger(data.logId))
        try:
            g.logger.info("Start job jobId=%s logId=%s [%s]" % (data.jobId, data.logId, data))
            timeout = data.executorTimeout or self.config.task_timeout
            if handler.is_async:
                result = await handler.start_async(timeout)
            else:
                result = await handler.start_sync(self.loop, self.thread_pool, timeout)
            g.logger.info("Job finished jobId=%s logId=%s" % (data.jobId, data.logId))
            await self.xxl_client.callback(data.logId, start_time, code=200, msg=result)
            self.successd_callback()
        except asyncio.CancelledError as e:
            g.logger.warning(e, exc_info=True)
            await self.xxl_client.callback(data.logId, start_time, code=500, msg="CancelledError")
            self.failed_callback("cancelled")
        except asyncio.exceptions.TimeoutError as e:
            # 同步任务run_in_executor超时会抛出TimeoutError异常
            # 但是注意线程里面的任务仍然在允许，可能会占满所有的线程池
            # todo: 杀死线程
            g.logger.warning(e, exc_info=True)
            await self.xxl_client.callback(data.logId, start_time, code=500, msg="TimeoutError")
            self.failed_callback("timeout")
        except Exception as err:  # pylint: disable=broad-except
            g.logger.exception(err)
            await self.xxl_client.callback(data.logId, start_time, code=500, msg=str(err))
            self.failed_callback("exception")
        finally:
            await self._finish(data.jobId)

    async def _finish(self, job_id: int) -> None:
        finish_task = self.tasks.pop(job_id, None)
        logger.info("Finish task {}".format(finish_task))
        # 如果有队列中的任务，开始执行队列中的任务
        queue = self.queue[job_id]
        if queue:
            kwargs: RunData = queue.pop(0)
            logger.info("JobId %s in queue[%s], start job with logId %s" % (kwargs.jobId, len(queue), kwargs.logId))
            await self.run_job(kwargs)

    async def _cancel(self, job_id: int) -> None:
        task = self.tasks.pop(job_id, None)
        if task:
            task.cancel()
            try:
                await task.task
            except asyncio.CancelledError:
                logger.warning("Job %s cancelled." % job_id)

    async def graceful_close(self, timeout: int = 60) -> None:
        """优雅关闭"""

        async def _graceful_close() -> None:
            while len(self.tasks) > 0:
                await asyncio.wait([i.task for i in self.tasks.values()])

        await asyncio.wait_for(_graceful_close(), timeout=timeout)

    def reset_handler(self, handler: Optional[JobHandler] = None) -> None:
        self.handler = handler or JobHandler()

    @property
    def register(self) -> Any:
        return self.handler.register
