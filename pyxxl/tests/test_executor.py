import asyncio
import time

import pytest

from pyxxl.enum import executorBlockStrategy
from pyxxl.error import JobDuplicateError, JobNotFoundError, JobParamsError
from pyxxl.executor import Executor, JobHandler
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
async def test_runner_callback(executor: Executor):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
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
                jobId=1100,
                executorHandler="pytest_executor_5",
                executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
            )
        )
    )
    await executor.run_job(
        RunData(
            **dict(
                logId=12,
                jobId=1200,
                executorHandler="pytest_executor_5",
                executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
            )
        )
    )
    await executor.cancel_job(1100)
    await executor.graceful_close()
    assert executor.xxl_client.callback_result.get(11) is None
    assert executor.xxl_client.callback_result.get(12) == 200


@pytest.mark.asyncio
async def test_runner_SERIAL_EXECUTION(executor: Executor, job_id: int):
    executor.xxl_client.clear_result()
    executor.reset_handler(job_handler)
    run_data = dict(
        jobId=job_id,
        executorHandler="pytest_executor_3",
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
async def test_runner_DISCARD_LATER(executor: Executor, job_id: int):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    run_data = dict(
        jobId=job_id,
        executorHandler="pytest_executor_3",
        executorBlockStrategy=executorBlockStrategy.DISCARD_LATER.value,
    )
    await executor.run_job(RunData(logId=11, **run_data))
    with pytest.raises(JobDuplicateError):
        await executor.run_job(RunData(logId=21, **run_data))
    await executor.graceful_close()
    assert executor.xxl_client.callback_result.get(11) == 200
    assert executor.xxl_client.callback_result.get(21) is None


@pytest.mark.asyncio
async def test_runner_COVER_EARLY(executor: Executor, job_id: int):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    run_data = dict(
        jobId=job_id,
        executorHandler="pytest_executor_3",
        executorBlockStrategy=executorBlockStrategy.COVER_EARLY.value,
    )
    await executor.run_job(RunData(logId=40, **run_data))
    await executor.run_job(RunData(logId=41, **run_data))
    await executor.graceful_close()
    assert executor.xxl_client.callback_result.get(41) == 200
    assert executor.xxl_client.callback_result.get(40) is None


@pytest.mark.asyncio
async def test_runner_OTHER(executor: Executor, job_id: int):
    executor.reset_handler(job_handler)
    with pytest.raises(JobParamsError, match="unknown executorBlockStrategy"):
        for i in range(2):
            await executor.run_job(
                RunData(
                    logId=40 + i,
                    jobId=job_id,
                    executorHandler="pytest_executor_3",
                    executorBlockStrategy="OTHER",
                )
            )
    executor.xxl_client.clear_result()
