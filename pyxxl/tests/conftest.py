import asyncio

from typing import Generator

import pytest
import pytest_asyncio

from aiohttp.web import Application
from pytest_aiohttp.plugin import AiohttpClient, TestClient

from pyxxl.execute import Executor
from pyxxl.tests.utils import MokePyxxlRunner, MokeXXL


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def executor() -> Executor:
    return Executor(MokeXXL("http://localhost:8080/xxl-job-admin/api/"))


@pytest.fixture(scope="session")
def web_app() -> Application:
    runner = MokePyxxlRunner(
        "http://localhost:8080/xxl-job-admin/api/",
        executor_name="xxl-job-executor-sample",
        port=9999,
        host="127.0.0.1",
    )

    @runner.handler.register(name="demoJobHandler")
    async def test_task() -> None:
        await asyncio.sleep(60)

    return runner.create_server_app()


@pytest_asyncio.fixture
async def cli(aiohttp_client: AiohttpClient, web_app: Application) -> TestClient:
    return await aiohttp_client(web_app)
