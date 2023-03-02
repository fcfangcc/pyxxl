import asyncio
import time
from typing import Any, Generator

import pytest
import pytest_asyncio
from aiohttp.web import Application
from pytest_aiohttp.plugin import AiohttpClient, TestClient

from pyxxl import ExecutorConfig
from pyxxl.executor import Executor
from pyxxl.tests.utils import INSTALL_REDIS, REDIS_TEST_URI, MokePyxxlRunner, MokeXXL

GLOBAL_JOB_ID = 1
GLOBAL_CONFIG: Any = dict(
    xxl_admin_baseurl="http://localhost:8080/xxl-job-admin/api/",
    executor_app_name="xxl-job-executor-sample",
    executor_host="127.0.0.1",
    graceful_close=False,
)

xxl_admin_baseurl = "http://localhost:8080/xxl-job-admin/api/"


def _create_job_id() -> int:
    global GLOBAL_JOB_ID
    GLOBAL_JOB_ID += 1
    return GLOBAL_JOB_ID


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(
    scope="session",
    params=[
        ExecutorConfig(**GLOBAL_CONFIG),
        pytest.param(
            ExecutorConfig(**GLOBAL_CONFIG, log_target="redis", log_redis_uri=REDIS_TEST_URI),
            marks=pytest.mark.skipif(not INSTALL_REDIS, reason="no redis package."),
        ),
    ],
    ids=["disk", "redis"],
)
def executor(request: Any) -> Executor:
    print(type(request))
    return Executor(MokeXXL("http://localhost:8080/xxl-job-admin/api/"), request.param, handler=None)


@pytest.fixture(scope="session")
def web_app(executor: Executor) -> Application:
    runner = MokePyxxlRunner(executor.config)

    @runner.handler.register(name="demoJobHandler")
    async def test_task() -> None:
        await asyncio.sleep(20)

    @runner.handler.register(name="demoJobHandlerSync")
    def test_task_sync() -> None:
        time.sleep(5)

    return runner.create_server_app()


@pytest_asyncio.fixture
async def cli(aiohttp_client: AiohttpClient, web_app: Application) -> TestClient:
    return await aiohttp_client(web_app)


@pytest.fixture(scope="function")
def job_id() -> int:
    return _create_job_id()
