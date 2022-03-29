from pathlib import Path
import sys
import pytest

sys.path.insert(0, Path(__file__).parent.parent.parent.absolute().as_posix())
# pylint: disable=wrong-import-position,wrong-import-order
from pyxxl.execute import Executor
from pyxxl.xxl_client import XXL


class MokeXXL(XXL):
    callback_result = {}

    async def callback(self, log_id: int, timestamp: int, code: int = 200, msg: str = None):
        self.callback_result[log_id] = code


@pytest.fixture
async def executor() -> Executor:
    return Executor(MokeXXL(""))
