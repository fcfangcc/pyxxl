import pytest

from pyxxl.error import JobRegisterError
from pyxxl.executor import Executor


# pylint: disable=function-redefined
@pytest.mark.asyncio
async def test_hander_error(executor: Executor):
    executor.reset_handler()
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
    executor.reset_handler()

    @executor.register
    def test_hander1():
        ...

    @executor.register(replace=True)
    async def test_hander1():  # noqa: F811
        ...

    @executor.register(name="test_hander1_dup")
    def test_hander1():  # noqa: F811
        ...
