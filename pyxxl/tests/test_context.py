import pytest

from pyxxl import job_hander
from pyxxl.execute import Executor
from pyxxl.enum import executorBlockStrategy
from pyxxl.schema import RunData
from pyxxl.ctx import g


@job_hander
async def text_ctx():
    logId = g.xxl_run_data.logId
    assert logId == 1


@pytest.mark.asyncio
async def test_runner_callback(executor: Executor):
    data = RunData(
        **dict(
            logId=1,
            jobId=11,
            executorHandler="text_ctx",
            executorBlockStrategy=executorBlockStrategy.SERIAL_EXECUTION.value,
        )
    )
    await executor.run_job(data)
    await executor.graceful_close()
    assert executor.xxl_client.callback_result.get(1) == 200
