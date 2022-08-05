import time

import pytest

from aiohttp.test_utils import TestClient


# import logging

# logger = logging.getLogger("pyxxl")
# handler = logging.StreamHandler()
# logger.addHandler(handler)
# logger.setLevel(logging.DEBUG)


async def _send_demoJobHandler(cli: TestClient, **kwargs):
    job_data = {
        "jobId": int(time.time() * 1000),
        "executorHandler": "demoJobHandler",
        "executorParams": "demoJobHandler",
        "executorBlockStrategy": "COVER_EARLY",
        "executorTimeout": 0,
        "logId": int(time.time() * 1000),
        "logDateTime": 1586629003729,
        "glueType": "BEAN",
        "glueSource": "xxx",
        "glueUpdatetime": 1586629003727,
        "broadcastIndex": 0,
        "broadcastTotal": 0,
    }
    job_data.update(kwargs)
    resp = await cli.post("/run", json=job_data)
    return resp, job_data["jobId"]


@pytest.mark.asyncio
async def test_run(cli: TestClient):
    resp, _ = await _send_demoJobHandler(cli, executorBlockStrategy="DISCARD_LATER", jobId=100)
    assert resp.status == 200
    assert await resp.json() == {"code": 200, "msg": None}
    # error
    resp, _ = await _send_demoJobHandler(cli, executorBlockStrategy="DISCARD_LATER", jobId=100)
    assert resp.status == 200
    response_dict = await resp.json()
    assert response_dict["code"] == 500
    assert "already executing" in response_dict["msg"]


@pytest.mark.asyncio
async def test_run_not_found(cli: TestClient):
    resp, _ = await _send_demoJobHandler(cli, executorHandler="test_run_not_found")
    assert resp.status == 200
    response_dict = await resp.json()
    assert response_dict["code"] == 500
    assert "not found" in response_dict["msg"]


@pytest.mark.asyncio
async def test_beat(cli: TestClient):
    resp = await cli.post("/beat")
    assert resp.status == 200
    assert await resp.json() == {"code": 200, "msg": None}


@pytest.mark.asyncio
async def test_idle_beat(cli: TestClient):
    resp = await cli.post("/idleBeat", json={"jobId": 1})
    assert resp.status == 200
    assert await resp.json() == {"code": 200, "msg": None}

    resp, jobId = await _send_demoJobHandler(cli, jobId=300)
    resp, jobId = await _send_demoJobHandler(cli, jobId=300)
    resp = await cli.post("/idleBeat", json={"jobId": jobId})
    response_data = await resp.json()
    assert response_data["code"] == 500
    assert response_data["msg"] == "job %s is running." % jobId


@pytest.mark.asyncio
async def test_kill(cli: TestClient):
    resp, jobId = await _send_demoJobHandler(cli)
    resp = await cli.post("/kill", json={"jobId": jobId})
    assert await resp.json() == {"code": 200, "msg": None}


@pytest.mark.asyncio
async def test_log(cli: TestClient):
    resp, jobId = await _send_demoJobHandler(cli)
    resp = await cli.post("/log", json={"jobId": jobId})
    assert (await resp.json())["code"] == 200
