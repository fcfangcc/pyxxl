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

GLOBAL_CONFIG: Any = dict(
    xxl_admin_baseurl="http://localhost:8080/xxl-job-admin/api/",
    executor_app_name="xxl-job-executor-sample",
    executor_listen_host="127.0.0.1",
    executor_listen_port=9999,
    graceful_close=False,
    task_queue_length=5,
    dotenv_try=False,
)

xxl_admin_baseurl = "http://localhost:8080/xxl-job-admin/api/"


def _generate_increment_id(start: int = 0) -> Generator[int, None, None]:
    while True:
        yield start
        start += 1


JOB_ID = _generate_increment_id()
LOG_ID = _generate_increment_id(1000)


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
    return Executor(MokeXXL("http://localhost:8080/xxl-job-admin/api/"), request.param, handler=None)


@pytest.fixture(scope="session")
def web_app(executor: Executor) -> Application:
    runner = MokePyxxlRunner(executor.config)

    @runner.register(name="demoJobHandler")
    async def test_task() -> None:
        await asyncio.sleep(10)

    @runner.register(name="demoJobHandlerSync")
    def test_task_sync() -> None:
        time.sleep(5)

    return runner.create_server_app()


@pytest_asyncio.fixture
async def cli(aiohttp_client: AiohttpClient, web_app: Application) -> TestClient:
    return await aiohttp_client(web_app)


@pytest.fixture(scope="function")
def job_id() -> int:
    return next(JOB_ID)


@pytest.fixture(scope="session")
def log_id_iter() -> Generator[int, None, None]:
    """Generate log_id for each test case."""
    return LOG_ID


@pytest.fixture(scope="function")
def log_id() -> int:
    return next(LOG_ID)
