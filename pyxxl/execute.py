from typing import Dict, List
import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from pyxxl import error
from pyxxl.xxl_client import XXL
from pyxxl.ctx import g
from pyxxl.enum import executorBlockStrategy
from pyxxl.schema import RunData, HandlerInfo

logger = logging.getLogger("pyxxl")


class Executor:

    def __init__(
        self,
        xxl_client: XXL,
        *,
        handlers=None,
        loop=None,
        max_workers=20,
        task_timeout=60 * 60,
        max_queue_length=30,
    ):
        self.xxl_client = xxl_client
        self.loop = loop or asyncio.get_event_loop()
        self.tasks: Dict[int, asyncio.Task] = {}
        self.queue: Dict[int, List[RunData]] = defaultdict(list)
        self.lock = asyncio.Lock()
        self.handlers = handlers
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="pyxxl_pool")
        self.task_timeout = task_timeout
        # 串行队列的最长长度
        self.max_queue_length = max_queue_length

    async def shutdown(self):
        for _, task in self.tasks.items():
            task.cancel()

    async def run_job(self, run_data: RunData):
        handler_obj: HandlerInfo = self.handlers.get(run_data.executorHandler, None)
        if not handler_obj:
            logger.warning("handler %s not found." % run_data.executorHandler)
            raise error.JobNotFoundError("handler %s not found." % run_data.executorHandler)

        # 一个执行器同时只能执行一个jobId相同的任务
        async with self.lock:
            current_task = self.tasks.get(run_data.jobId)
            if current_task:
                # 不用的阻塞策略
                # pylint: disable=no-else-raise
                if run_data.executorBlockStrategy == executorBlockStrategy.DISCARD_LATER.value:
                    raise error.JobDuplicateError(
                        "The same job [%s] is already executing and this has been discarded." % run_data.jobId
                    )
                elif run_data.executorBlockStrategy == executorBlockStrategy.COVER_EARLY.value:
                    logger.warning("job %s is  COVER_EARLY, logId %s replaced." % (run_data.jobId, run_data.logId))
                    await self._cancel(run_data.jobId)
                elif run_data.executorBlockStrategy == executorBlockStrategy.SERIAL_EXECUTION.value:

                    if len(self.queue[run_data.jobId]) > self.max_queue_length:
                        msg = "job %s is  SERIAL, queue length more than %s. logId %s  discard !" % (
                            run_data.jobId, self.max_queue_length, run_data.logId
                        )
                        logger.error(msg)
                        raise error.JobDuplicateError(msg)
                    else:
                        queue = self.queue[run_data.jobId]
                        logger.info(
                            "job %s is in queen, logId %s wait for %s..." %
                            (run_data.jobId, run_data.logId, len(queue) + 1)
                        )
                        queue.append(run_data)
                        return
                else:
                    raise error.JobDuplicateError(
                        "unknown executorBlockStrategy [%s]." % run_data.executorBlockStrategy
                    )

            start_time = int(time.time()) * 1000
            task = self.loop.create_task(self._run(handler_obj, start_time, run_data))
            self.tasks[run_data.jobId] = task

    async def cancel_job(self, job_id: int):
        async with self.lock:
            await self._cancel(job_id)

    async def is_running(self, job_id: int):
        return job_id in self.tasks

    async def _run(self, handler: HandlerInfo, start_time, data: RunData):
        try:
            g.set_xxl_run_data(data)
            logger.info("start job %s %s" % (data.jobId, data))
            func = handler.handler() if handler.is_async else self.loop.run_in_executor(
                self.thread_pool,
                handler.handler,
            )
            result = await asyncio.wait_for(func, self.task_timeout)
            logger.info("end job %s %s" % (data.jobId, data))
            await self.xxl_client.callback(data.logId, start_time, code=200, msg=result)
        except asyncio.CancelledError:
            await self.xxl_client.callback(data.logId, start_time, code=500, msg="CancelledError")
        except Exception as err:  # pylint: disable=broad-except
            logger.exception(err)
            await self.xxl_client.callback(data.logId, start_time, code=500, msg=str(err))
        finally:
            await self._finish(data.jobId)

    async def _finish(self, job_id: int):
        async with self.lock:
            self.tasks.pop(job_id, None)
        # 如果有队列中的任务，开始执行队列中的任务
        queue = self.queue[job_id]
        if queue:
            kwargs: RunData = queue.pop(0)
            logger.info("job %s in queue[%s], start job with logId %s" % (kwargs.jobId, len(queue), kwargs.logId))
            await self.run_job(kwargs)

    async def _cancel(self, job_id: int):
        task = self.tasks.pop(job_id, None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.warning("Job %s cancelled." % job_id)

    async def graceful_close(self, timeout=60):

        async def _graceful_close():
            while len(self.tasks) > 0:
                await asyncio.wait(self.tasks.values())

        await asyncio.wait_for(_graceful_close(), timeout=timeout)
