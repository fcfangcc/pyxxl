import asyncio
import time
from typing import Iterator

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
async def test_runner_not_found(executor: Executor, job_id: int, log_id: int):
    executor.reset_handler(job_handler)
    with pytest.raises(JobNotFoundError):
        await executor.run_job(
            RunData.from_dict(
                dict(
                    logId=log_id,
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
async def test_runner_cancel(executor: Executor, handler_name: str):
    executor.reset_handler(job_handler)
    cancel_job_id, ok_job_id = 1100, 1200
    cancel_log_id, ok_log_id = 1100, 1200
    base_data = dict(
        executorHandler=handler_name,
        executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
    )
    await executor.run_job(RunData(logId=cancel_log_id, jobId=cancel_job_id, **base_data))
    await executor.run_job(RunData(logId=ok_log_id, jobId=ok_job_id, **base_data))

    await executor.cancel_job(cancel_job_id, include_queue=False)
    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(cancel_job_id) == 500
    assert executor.xxl_client.callback_result.get(ok_job_id) == 200


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_cancel_include_queue(
    executor: Executor, handler_name: str, job_id: int, log_id_iter: Iterator[int]
):
    executor.reset_handler(job_handler)
    base_data = dict(
        jobId=job_id,
        executorHandler=handler_name,
        executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
    )
    cancel_log_id, queue_log_id = next(log_id_iter), next(log_id_iter)
    await executor.run_job(RunData(logId=cancel_log_id, **base_data))
    await executor.run_job(RunData(logId=queue_log_id, **base_data))
    await executor.cancel_job(job_id, include_queue=True)
    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(cancel_log_id) == 500
    assert executor.xxl_client.callback_result.get(queue_log_id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_SERIAL_EXECUTION(executor: Executor, job_id: int, handler_name: str, log_id_iter: Iterator[int]):
    executor.xxl_client.clear_result()
    executor.reset_handler(job_handler)
    run_data = dict(
        jobId=job_id,
        executorHandler=handler_name,
        executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
    )
    queue_size = 3
    log_ids = [next(log_id_iter) for _ in range(queue_size)]
    for log_id in log_ids:
        await executor.run_job(RunData(logId=log_id, **run_data))

    assert executor.queue.get(job_id).qsize() == queue_size - 1
    await executor.graceful_close(10)
    assert executor.queue.get(job_id).qsize() == 0
    assert executor.xxl_client.callback_result.get(log_id) == 200

    # max_queue_length
    for _ in range(executor.config.task_queue_length + 1):
        await executor.run_job(RunData(logId=next(log_id_iter), **run_data))

    with pytest.raises(JobDuplicateError, match="discard"):
        await executor.run_job(RunData(logId=next(log_id_iter), **run_data))

    await executor.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_DISCARD_LATER(executor: Executor, job_id: int, handler_name: str, log_id_iter: Iterator[int]):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    run_data = dict(
        jobId=job_id,
        executorHandler=handler_name,
        executorBlockStrategy=executorBlockStrategy.DISCARD_LATER.value,
    )
    ok_log_id, duplicate_log_id = next(log_id_iter), next(log_id_iter)
    await executor.run_job(RunData(logId=ok_log_id, **run_data))
    with pytest.raises(JobDuplicateError):
        await executor.run_job(RunData(logId=duplicate_log_id, **run_data))
    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(ok_log_id) == 200
    assert executor.xxl_client.callback_result.get(duplicate_log_id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_COVER_EARLY(executor: Executor, job_id: int, handler_name: str, log_id_iter: Iterator[int]):
    executor.reset_handler(job_handler)
    executor.xxl_client.clear_result()
    run_data = dict(
        jobId=job_id,
        executorHandler=handler_name,
        executorBlockStrategy=executorBlockStrategy.COVER_EARLY.value,
    )
    ok_log_id, error_log_id = next(log_id_iter), next(log_id_iter)
    await executor.run_job(RunData(logId=error_log_id, **run_data))
    await executor.run_job(RunData(logId=ok_log_id, **run_data))
    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(ok_log_id) == 200
    assert executor.xxl_client.callback_result.get(error_log_id) == 500


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", HANDLER_NAMES)
async def test_runner_OTHER(executor: Executor, job_id: int, handler_name: str, log_id_iter: Iterator[int]):
    executor.reset_handler(job_handler)
    with pytest.raises(JobParamsError, match="unknown executorBlockStrategy"):
        for _ in range(2):
            await executor.run_job(
                RunData(
                    logId=next(log_id_iter),
                    jobId=job_id,
                    executorHandler=handler_name,
                    executorBlockStrategy="OTHER",
                )
            )
    executor.xxl_client.clear_result()


@pytest.mark.asyncio
async def test_sync_timeout(executor: Executor, job_id: int, log_id: int):
    from pyxxl.ctx import g

    sync_handler = JobHandler()

    @sync_handler.register(name="pytest_executor_sync")
    def pytest_executor_sync():
        while not g.cancel_event.is_set():
            time.sleep(1)

    executor.reset_handler(sync_handler)
    await executor.run_job(
        RunData(
            logId=log_id,
            jobId=job_id,
            executorHandler="pytest_executor_sync",
            executorBlockStrategy="OTHER",
            executorTimeout=2,
        )
    )
    await executor.graceful_close(10)
    assert executor.xxl_client.callback_result.get(log_id) == 500
