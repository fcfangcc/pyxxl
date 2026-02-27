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


def _spawn_task(task: asyncio.Task) -> None:
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
        # 为每个jobId创建独立的锁，避免不同job之间的锁竞争
        self._job_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
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

    def _get_job_lock(self, job_id: int) -> asyncio.Lock:
        """获取指定jobId的锁"""
        return self._job_locks[job_id]

    def _create_task(self, data: RunData) -> XXLTask:
        """创建一个任务"""
        task = self.loop.create_task(self._run(data), name=f"{data.jobId}_{data.logId}")
        return XXLTask(task, data)

    async def _handle_discard_later(self, data: RunData) -> str:
        """处理DISCARD_LATER策略：丢弃后来的任务"""
        raise error.JobDuplicateError("The same job [%s] is already executing and this has been discarded." % data)

    async def _handle_cover_early(self, data: RunData) -> str:
        """处理COVER_EARLY策略：覆盖早期任务"""
        msg = "Job {} BlockStrategy is COVER_EARLY, logId {} replaced.".format(data.jobId, data.logId)
        self.executor_logger.warning(msg)
        await self.get_queue(data.jobId).put(data)
        _spawn_task(self.loop.create_task(self.cancel_job(data.jobId, include_queue=False)))
        return msg

    async def _handle_serial_execution(self, data: RunData) -> str:
        """处理SERIAL_EXECUTION策略：串行执行，加入队列"""
        queue = self.get_queue(data.jobId)
        if queue.full():
            msg = "Job {job_id} is SERIAL, queue length more than {maxsize}. Job {job} discard!".format(
                job_id=data.jobId, job=data, maxsize=queue.maxsize
            )
            self.executor_logger.error(msg)
            raise error.JobDuplicateError(msg)
        else:
            msg = "job {job_id} is in queue, logId {log_id} ranked {ranked}th [max={maxsize}]...".format(
                job_id=data.jobId, log_id=data.logId, ranked=queue.qsize() + 1, maxsize=queue.maxsize
            )
            self.executor_logger.info(msg)
            await queue.put(data)
            return msg

    async def run_job(self, data: RunData) -> str:
        handler_obj = self.handler.get(data.executorHandler)
        if not handler_obj:
            self.executor_logger.warning("handler %s not found." % data.executorHandler)
            raise error.JobNotFoundError("handler %s not found." % data.executorHandler)

        # 使用jobId对应的锁，避免全局锁竞争
        job_lock = self._get_job_lock(data.jobId)
        async with job_lock:
            # 检查该jobId是否正在运行或队列中有任务
            current_task = self.tasks.get(data.jobId)
            queue = self.get_queue(data.jobId)

            # 如果没有运行任务且队列为空，直接创建并运行
            if not current_task and queue.empty():
                self.tasks[data.jobId] = self._create_task(data)
                return "Running"

            # 任务冲突，根据阻塞策略处理
            self.executor_logger.warning("jobId {} is running. current_task={}".format(data.jobId, current_task))

            if data.executorBlockStrategy == executorBlockStrategy.DISCARD_LATER.value:
                return await self._handle_discard_later(data)
            elif data.executorBlockStrategy == executorBlockStrategy.COVER_EARLY.value:
                return await self._handle_cover_early(data)
            elif data.executorBlockStrategy == executorBlockStrategy.SERIAL_EXECUTION.value:
                return await self._handle_serial_execution(data)
            else:
                raise error.JobParamsError(
                    "unknown executorBlockStrategy [%s]." % data.executorBlockStrategy,
                    executorBlockStrategy=data.executorBlockStrategy,
                )

    async def cancel_job(self, job_id: int, include_queue: bool = True) -> None:
        await asyncio.sleep(0.01)  # delay for pytest
        self.executor_logger.warning("start kill job: job_id={}".format(job_id))

        job_lock = self._get_job_lock(job_id)
        task_to_cancel = None

        # 在锁内进行队列清理和任务标记
        async with job_lock:
            # 清空队列
            if include_queue:
                queue = self.get_queue(job_id)
                while not queue.empty():
                    data = queue.get_nowait()
                    self.executor_logger.warning("Discard jobId {} from queue, data: {}".format(job_id, data))

            # 获取需要取消的任务
            task_to_cancel = self.tasks.get(job_id, None)
            if task_to_cancel:
                task_to_cancel.cancel()

        # 在锁外等待任务完成，避免死锁
        # 因为任务的finally块中也需要获取同一个锁
        if task_to_cancel:
            try:
                # https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.cancel
                await task_to_cancel.task
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
                # 使用jobId对应的锁来保护finish操作
                job_lock = self._get_job_lock(data.jobId)
                async with job_lock:
                    await self._finish(data.jobId)

    async def _finish(self, job_id: int) -> None:
        finish_task = self.tasks.pop(job_id, None)
        self.executor_logger.info("Finish task {}".format(finish_task))

        # 检查队列中是否还有等待的任务
        queue = self.get_queue(job_id)
        if not queue.empty():
            data = queue.get_nowait()
            self.executor_logger.info(
                "Get data from queue jobId={}, after queueSize={}, data={}".format(job_id, queue.qsize(), data)
            )
            # 启动队列中的下一个任务
            self.tasks[job_id] = self._create_task(data)
            queue.task_done()

    async def shutdown(self) -> None:
        """立即关闭执行器，取消所有任务"""
        await asyncio.sleep(0.01)  # sleep for pytest

        # 清空所有队列
        self.queue.clear()

        # 取消所有正在运行的任务
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
