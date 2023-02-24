import asyncio
import time

import pytest

from pyxxl.enum import executorBlockStrategy
from pyxxl.error import JobDuplicateError, JobNotFoundError, JobParamsError
from pyxxl.executor import Executor, JobHandler
from pyxxl.schema import RunData

job_handler = JobHandler()


@job_handler.register
async def pytest_executor_async():
    await asyncio.sleep(3)
    return "成功30"


@job_handler.register
def pytest_executor_sync():
    time.sleep(3)
    return "成功30"


@job_handler.register
async def pytest_executor_error():
    assert 1 == 2


HANDLER_NAMES = [
    "pytest_executor_async",
    "pytest_executor_sync",
]


@pytest.mark.asyncio
async def test_runner_not_found(executor: Executor, job_id: int):
    executor.reset_handler(job_handler)
    with pytest.raises(JobNotFoundError):
        await executor.run_job(
            RunData(
                **dict(
                    logId=211,
                    jobId=job_id,
                    executorHandler="not_found",
                    executorBlockStrategy=executorBlockStrategy.DISCARD_LATER.value,
                )
            )
        )
    await executor.graceful_close()


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_callback(executor: Executor, handler_name: str):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    data = RunData(
        **dict(
            logId=1,
            jobId=11,
            executorHandler=handler_name,
            executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
        )
    )
    await executor.run_job(data)
    data = RunData(
        **dict(
            logId=2,
            jobId=12,
            executorHandler="pytest_executor_error",
            executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
        )
    )
    await executor.run_job(data)
    await executor.graceful_close()
    assert executor.xxl_client.callback_result.get(1) == 200
    assert executor.xxl_client.callback_result.get(2) == 500
    assert executor.xxl_client.callback_result.get(3) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_cancel(executor: Executor, handler_name: str):
    executor.reset_handler(job_handler)
    cancel_job_id, ok_job_id = 1100, 1200
    await executor.run_job(
        RunData(
            **dict(
                logId=cancel_job_id,
                jobId=cancel_job_id,
                executorHandler=handler_name,
                executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
            )
        )
    )
    await executor.run_job(
        RunData(
            **dict(
                logId=ok_job_id,
                jobId=ok_job_id,
                executorHandler=handler_name,
                executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
            )
        )
    )
    await executor.cancel_job(cancel_job_id)
    await executor.graceful_close()
    assert executor.xxl_client.callback_result.get(cancel_job_id) is None
    assert executor.xxl_client.callback_result.get(ok_job_id) == 200


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_SERIAL_EXECUTION(executor: Executor, job_id: int, handler_name: str):
    executor.xxl_client.clear_result()
    executor.reset_handler(job_handler)
    run_data = dict(
        jobId=job_id,
        executorHandler=handler_name,
        executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
    )
    await executor.run_job(RunData(logId=11, **run_data))
    await executor.run_job(RunData(logId=12, **run_data))
    await executor.run_job(RunData(logId=13, **run_data))
    assert len(executor.queue.get(job_id)) == 2
    await executor.graceful_close()
    assert len(executor.queue.get(job_id)) == 0
    assert executor.xxl_client.callback_result.get(13) == 200

    # max_queue_length
    executor.config.task_queue_length = 2
    await executor.run_job(RunData(logId=11, **run_data))
    await executor.run_job(RunData(logId=12, **run_data))
    await executor.run_job(RunData(logId=13, **run_data))
    with pytest.raises(JobDuplicateError, match="discard"):
        await executor.run_job(RunData(logId=14, **run_data))
    #
    executor.config.task_queue_length = 30


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_DISCARD_LATER(executor: Executor, job_id: int, handler_name: str):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    run_data = dict(
        jobId=job_id,
        executorHandler=handler_name,
        executorBlockStrategy=executorBlockStrategy.DISCARD_LATER.value,
    )
    await executor.run_job(RunData(logId=11, **run_data))
    with pytest.raises(JobDuplicateError):
        await executor.run_job(RunData(logId=21, **run_data))
    await executor.graceful_close()
    assert executor.xxl_client.callback_result.get(11) == 200
    assert executor.xxl_client.callback_result.get(21) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_COVER_EARLY(executor: Executor, job_id: int, handler_name: str):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    run_data = dict(
        jobId=job_id,
        executorHandler=handler_name,
        executorBlockStrategy=executorBlockStrategy.COVER_EARLY.value,
    )
    await executor.run_job(RunData(logId=40, **run_data))
    await executor.run_job(RunData(logId=41, **run_data))
    await executor.graceful_close()
    assert executor.xxl_client.callback_result.get(41) == 200
    assert executor.xxl_client.callback_result.get(40) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_OTHER(executor: Executor, job_id: int, handler_name: str):
    executor.reset_handler(job_handler)
    with pytest.raises(JobParamsError, match="unknown executorBlockStrategy"):
        for i in range(2):
            await executor.run_job(
                RunData(
                    logId=40 + i,
                    jobId=job_id,
                    executorHandler=handler_name,
                    executorBlockStrategy="OTHER",
                )
            )
    executor.xxl_client.clear_result()
