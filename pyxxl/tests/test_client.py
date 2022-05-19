import pytest

from pyxxl.xxl_client import XXL


@pytest.mark.asyncio
async def test_param_admin_url():
    XXL("http://localhost:8080/xxl-job-admin/api/")
    XXL("https://localhost:8080/xxl-job-admin/api/")

    with pytest.raises(ValueError):
        XXL("htp://localhost:8080/xxl-job-admin/api/")

    with pytest.raises(ValueError):
        XXL("http://localhost:8080/xxl-job-admin/api")
