import pytest

from pyxxl.error import JobRegisterError
from pyxxl.executor import Executor


# pylint: disable=function-redefined
@pytest.mark.asyncio
async def test_hander_error(executor: Executor):
    with pytest.raises(JobRegisterError):

        @executor.handler.register
        def test_dup_error():
            ...

        @executor.handler.register
        def test_dup_error():  # noqa: F811
            ...


# pylint: disable=function-redefined
@pytest.mark.asyncio
async def test_hander(executor: Executor):
    @executor.handler.register
    def text_hander1():
        ...

    @executor.handler.register(replace=True)
    async def text_hander1():  # noqa: F811
        ...

    @executor.handler.register(name="text_hander_dup")
    def text_hander1():  # noqa: F811
        ...
