import pytest

from pyxxl import job_hander
from pyxxl.error import JobRegisterError


@pytest.mark.asyncio
async def test_hander_error():
    with pytest.raises(JobRegisterError):

        @job_hander
        def text_ctx():
            return 1

        @job_hander
        def test_ctx():
            return 1


# pylint: disable=function-redefined
@pytest.mark.asyncio
async def test_hander():

    @job_hander
    def text_hander1():
        return 1

    @job_hander(replace=True)
    async def text_hander1():
        return 1

    @job_hander(name="text_hander_dup")
    def text_hander1():
        return 1