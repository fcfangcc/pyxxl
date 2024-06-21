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
    await asyncio.sleep(2)
    return "成功30"


@job_handler.register
def pytest_executor_sync():
    time.sleep(2)
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
            RunData.from_dict(
                dict(
                    logId=211,
                    jobId=job_id,
                    executorHandler="not_found",
                    executorBlockStrategy=executorBlockStrategy.DISCARD_LATER.value,
                    errorTest="errorTest",
                )
            )
        )
    await executor.graceful_close()


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_callback(executor: Executor, handler_name: str):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    data = RunData.from_dict(
        dict(
            logId=1,
            jobId=11,
            executorHandler=handler_name,
            executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
        )
    )
    await executor.run_job(data)
    data = RunData.from_dict(
        dict(
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
async def test_runner_cancel(executor: Executor, handler_name: str, job_id: int):
    executor.reset_handler(job_handler)
    cancel_job_id, ok_job_id = 1100, 1200
    base_data = dict(
        executorHandler=handler_name,
        executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
    )
    await executor.run_job(RunData(logId=cancel_job_id, jobId=cancel_job_id, **base_data))
    await executor.run_job(RunData(logId=ok_job_id, jobId=ok_job_id, **base_data))

    await executor.cancel_job(cancel_job_id, include_queue=False)
    await executor.graceful_close()
    assert executor.xxl_client.callback_result.get(cancel_job_id) == 500
    assert executor.xxl_client.callback_result.get(ok_job_id) == 200
    # include_queue =True
    base_data.update({"jobId": job_id})
    executor.xxl_client.clear_result()
    cancel_log_id = job_id
    queue_log_id = job_id + 1
    await executor.run_job(RunData(logId=cancel_log_id, **base_data))
    await executor.run_job(RunData(logId=queue_log_id, **base_data))
    await executor.cancel_job(job_id, include_queue=True)
    await executor.graceful_close()
    assert executor.xxl_client.callback_result.get(cancel_log_id) == 500
    assert executor.xxl_client.callback_result.get(queue_log_id) is None


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
    assert executor.queue.get(job_id).qsize() == 2
    await executor.graceful_close()
    assert executor.queue.get(job_id).qsize() == 0
    assert executor.xxl_client.callback_result.get(13) == 200

    # max_queue_length
    for i in range(executor.config.task_queue_length + 1):
        await executor.run_job(RunData(logId=100 + i, **run_data))

    with pytest.raises(JobDuplicateError, match="discard"):
        await executor.run_job(RunData(logId=101 + i, **run_data))

    await executor.shutdown()


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
    assert executor.xxl_client.callback_result.get(40) == 500


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
