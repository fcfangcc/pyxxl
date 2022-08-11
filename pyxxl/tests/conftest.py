import asyncio
import logging

from typing import Generator

import pytest
import pytest_asyncio

from aiohttp.web import Application
from pytest_aiohttp.plugin import AiohttpClient, TestClient

from pyxxl import ExecutorConfig
from pyxxl.executor import Executor
from pyxxl.tests.utils import MokePyxxlRunner, MokeXXL
from pyxxl.utils import setup_logging


setup_logging(logging.INFO)

GLOBAL_JOB_ID = 1


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


@pytest.fixture(scope="session")
def executor_config() -> ExecutorConfig:
    return ExecutorConfig(
        xxl_admin_baseurl="http://localhost:8080/xxl-job-admin/api/",
        executor_app_name="xxl-job-executor-sample",
        executor_host="127.0.0.1",
        graceful_close=False,
    )


@pytest.fixture(scope="session")
def executor(executor_config: ExecutorConfig) -> Executor:
    return Executor(MokeXXL("http://localhost:8080/xxl-job-admin/api/"), executor_config)


@pytest.fixture(scope="session")
def web_app(executor_config: ExecutorConfig) -> Application:

    runner = MokePyxxlRunner(executor_config)

    @runner.handler.register(name="demoJobHandler")
    async def test_task() -> None:
        await asyncio.sleep(60)

    return runner.create_server_app()


@pytest_asyncio.fixture
async def cli(aiohttp_client: AiohttpClient, web_app: Application) -> TestClient:
    return await aiohttp_client(web_app)


@pytest.fixture(scope="function")
def job_id() -> int:
    return _create_job_id()
