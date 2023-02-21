import pytest
from aiohttp import web
from pytest_aiohttp.plugin import AiohttpClient

from pyxxl.xxl_client import XXL


@pytest.mark.asyncio
async def test_param_admin_url():
    XXL("http://localhost:8080/xxl-job-admin/api/")
    XXL("https://localhost:8080/xxl-job-admin/api/")

    with pytest.raises(ValueError):
        XXL("htp://localhost:8080/xxl-job-admin/api/")

    with pytest.raises(ValueError):
        XXL("http://localhost:8080/xxl-job-admin/api")


@pytest.mark.asyncio
async def test_client(aiohttp_client: AiohttpClient) -> None:
    async def moke_registry_api(request: web.Request):
        data = await request.json()
        if data.get("registryKey") == "server_test":
            return web.json_response({"code": 500, "msg": "1"}, status=500)

        if data.get("registryKey") == "status_test":
            return web.json_response({"code": 500, "msg": "1"}, status=200)

        return web.json_response({"code": 200, "msg": "1"})

    async def moke_callback_api(request: web.Request):
        return web.json_response({"code": 200, "msg": "1"})

    app = web.Application()
    app.router.add_post("/xxl-job-admin/api/registry", moke_registry_api)
    app.router.add_post("/xxl-job-admin/api/callback", moke_callback_api)
    session = await aiohttp_client(app)
    xxl_client = XXL("http://localhost:8080/xxl-job-admin/api/", session=session)
    # registry
    assert await xxl_client.registry("key", "value")
    assert not (await xxl_client.registry("server_test", "value"))
    assert not (await xxl_client.registry("status_test", "value"))
    # callback
    await xxl_client.callback(123, 123123123)
