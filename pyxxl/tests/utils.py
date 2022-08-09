from typing import Any, Dict, Optional

from pyxxl.main import PyxxlRunner
from pyxxl.xxl_client import XXL, JsonType, Response


class MokeXXL(XXL):
    callback_result: Dict[int, Any] = {}

    async def callback(self, log_id: int, timestamp: int, code: int = 200, msg: str = None) -> None:
        self.callback_result[log_id] = code

    async def _post(self, path: str, payload: JsonType, retry_times: Optional[int] = None) -> Response:
        return Response(code=200)

    def clear_result(self) -> None:
        self.callback_result = {}


class MokePyxxlRunner(PyxxlRunner):
    def _get_xxl_clint(self) -> MokeXXL:
        return MokeXXL(self.config.xxl_admin_baseurl, token=self.config.access_token)
