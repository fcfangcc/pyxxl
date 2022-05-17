import asyncio
import time

import pytest

from pyxxl.enum import executorBlockStrategy
from pyxxl.error import JobDuplicateError, JobNotFoundError
from pyxxl.execute import Executor, JobHandler
from pyxxl.schema import RunData


job_handler = JobHandler()


@job_handler.register
async def pytest_executor_5():
    await asyncio.sleep(5)
    return "成功30"


@job_handler.register
async def pytest_executor_error():
    assert 1 == 2


@job_handler.register
def pytest_executor_3():
    time.sleep(3)
    return "成功30"


@pytest.mark.asyncio
async def test_runner_not_found(executor: Executor):
    executor.reset_handler(job_handler)
    with pytest.raises(JobNotFoundError):
        await executor.run_job(
            RunData(
                **dict(
                    logId=211,
                    jobId=211,
                    executorHandler="not_found",
                    executorBlockStrategy=executorBlockStrategy.DISCARD_LATER.value,
                )
            )
        )
    await executor.graceful_close()


@pytest.mark.asyncio
async def test_runner_callback(executor: Executor):
    executor.reset_handler(job_handler)
    data = RunData(
        **dict(
            logId=1,
            jobId=11,
            executorHandler="pytest_executor_5",
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
async def test_runner_cancel(executor: Executor):
    executor.reset_handler(job_handler)
    await executor.run_job(
        RunData(
            **dict(
                logId=11,
                jobId=11,
                executorHandler="pytest_executor_5",
                executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
            )
        )
    )
    await executor.run_job(
        RunData(
            **dict(
                logId=12,
                jobId=12,
                executorHandler="pytest_executor_5",
                executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
            )
        )
    )
    await executor.cancel_job(11)
    await executor.graceful_close()
    assert executor.xxl_client.callback_result.get(11) is None
    assert executor.xxl_client.callback_result.get(12) == 200


@pytest.mark.asyncio
async def test_runner_SERIAL_EXECUTION(executor: Executor):
    executor.reset_handler(job_handler)
    jobId = 11
    await executor.run_job(
        RunData(
            **dict(
                logId=11,
                jobId=jobId,
                executorHandler="pytest_executor_3",
                executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
            )
        )
    )
    await executor.run_job(
        RunData(
            **dict(
                logId=21,
                jobId=jobId,
                executorHandler="pytest_executor_3",
                executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
            )
        )
    )
    await executor.run_job(
        RunData(
            **dict(
                logId=31,
                jobId=jobId,
                executorHandler="pytest_executor_3",
                executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
            )
        )
    )
    assert len(executor.queue.get(jobId)) == 2
    await executor.graceful_close()
    assert len(executor.queue.get(jobId)) == 0
    assert executor.xxl_client.callback_result.get(31) == 200


@pytest.mark.asyncio
async def test_runner_DISCARD_LATER(executor: Executor):
    executor.reset_handler(job_handler)
    jobId = 31
    executor.xxl_client.callback_result.clear()
    await executor.run_job(
        RunData(
            **dict(
                logId=11,
                jobId=jobId,
                executorHandler="pytest_executor_3",
                executorBlockStrategy=executorBlockStrategy.DISCARD_LATER.value,
            )
        )
    )
    with pytest.raises(JobDuplicateError):
        await executor.run_job(
            RunData(
                **dict(
                    logId=21,
                    jobId=jobId,
                    executorHandler="pytest_executor_3",
                    executorBlockStrategy=executorBlockStrategy.DISCARD_LATER.value,
                )
            )
        )
    await executor.graceful_close()
    assert executor.xxl_client.callback_result.get(11) == 200
    assert executor.xxl_client.callback_result.get(21) is None


@pytest.mark.asyncio
async def test_runner_COVER_EARLY(executor: Executor):
    executor.reset_handler(job_handler)
    jobId = 41
    executor.xxl_client.callback_result.clear()
    await executor.run_job(
        RunData(
            **dict(
                logId=40,
                jobId=jobId,
                executorHandler="pytest_executor_3",
                executorBlockStrategy=executorBlockStrategy.COVER_EARLY.value,
            )
        )
    )
    await executor.run_job(
        RunData(
            **dict(
                logId=41,
                jobId=jobId,
                executorHandler="pytest_executor_3",
                executorBlockStrategy=executorBlockStrategy.COVER_EARLY.value,
            )
        )
    )
    await executor.graceful_close()
    assert executor.xxl_client.callback_result.get(41) == 200
    assert executor.xxl_client.callback_result.get(40) is None
