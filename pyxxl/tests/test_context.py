import pytest

from pyxxl.ctx import g
from pyxxl.enum import executorBlockStrategy
from pyxxl.executor import Executor
from pyxxl.schema import RunData


@pytest.mark.asyncio
async def test_runner_callback(executor: Executor):
    @executor.register
    async def test_ctx():
        logId = g.xxl_run_data.logId
        assert logId == 1

    @executor.register
    def test_ctx_sync():
        logId = g.xxl_run_data.logId
        assert logId == 1

    for handler in ["test_ctx", "test_ctx_sync"]:
        data = RunData(
            **dict(
                logId=1,
                jobId=11,
                executorHandler=handler,
                executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
            )
        )
        await executor.run_job(data)
        await executor.graceful_close()
        assert executor.xxl_client.callback_result.get(1) == 200
