import pytest

from pyxxl.error import JobRegisterError
from pyxxl.execute import Executor


# pylint: disable=function-redefined
@pytest.mark.asyncio
async def test_hander_error(executor: Executor):
    with pytest.raises(JobRegisterError):

        @executor.handler.register
        def test_dup_error():
            return 1

        @executor.handler.register
        def test_dup_error():
            return 1


# pylint: disable=function-redefined
@pytest.mark.asyncio
async def test_hander(executor: Executor):

    @executor.handler.register
    def text_hander1():
        return 1

    @executor.handler.register(replace=True)
    async def text_hander1():
        return 1

    @executor.handler.register(name="text_hander_dup")
    def text_hander1():
        return 1