import os
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional

from pyxxl.ctx import g
from pyxxl.main import PyxxlRunner
from pyxxl.schema import RunData
from pyxxl.utils import try_import
from pyxxl.xxl_client import XXL, JsonType, Response


class MokeXXL(XXL):
    callback_result: Dict[int, Any] = {}

    async def callback(self, log_id: int, timestamp: int, code: int = 200, msg: Optional[str] = None) -> None:
        self.callback_result[log_id] = code

    async def _post(self, path: str, payload: JsonType, retry_times: Optional[int] = None) -> Response:
        return Response(code=200)

    def clear_result(self) -> None:
        self.callback_result = {}


class MokePyxxlRunner(PyxxlRunner):
    def _get_xxl_clint(self) -> MokeXXL:
        return MokeXXL(self.config.xxl_admin_baseurl, token=self.config.access_token)


REDIS_TEST_URI = os.environ.get("REDIS_TEST_URI", "redis://localhost")
INSTALL_REDIS = bool(try_import("redis"))


@asynccontextmanager
async def mock_run_data(job_id: int, log_id: int) -> AsyncGenerator[None, None]:
    token = g.set_xxl_run_data(
        RunData(jobId=job_id, logId=log_id, executorHandler="mock", executorBlockStrategy="serial")
    )
    yield None
    g._DATA.reset(token)
