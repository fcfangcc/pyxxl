import asyncio

import pytest
from aiohttp.test_utils import TestClient

from pyxxl.utils import try_import

from .test_server import send_demoJobHandler


@pytest.mark.asyncio
@pytest.mark.skipif(not try_import("prometheus_client"), reason="不存在prometheus_client")
async def test_metrics(cli: TestClient):
    for _ in range(3):
        await send_demoJobHandler(cli, jobId=630)
        await send_demoJobHandler(cli, jobId=631, executorHandler="demoJobHandlerSync")
        await asyncio.sleep(0.01)
    await asyncio.sleep(1)
    resp = await cli.get("/metrics")
    assert resp.status == 200
    assert "python_gc_objects_collected_total" in await resp.text()
