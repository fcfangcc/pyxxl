import logging
import asyncio
import aiohttp
from pyxxl.error import XXLRegisterError, ClientError

logger = logging.getLogger("pyxxl")


class Response:
    # pylint: disable=unused-argument
    def __init__(self, code: int, msg: str = None, **kwargs):
        self.code = code
        self.msg = msg

    @property
    def ok(self):
        return self.code == 200


class XXL:

    def __init__(
        self,
        admin_url: str,
        token: str = None,
        loop=None,
        retry_times: int = 0,
        retry_interval: int = 5,
        **kwargs,
    ):
        self.loop = loop or asyncio.get_event_loop()
        kwargs['loop'] = self.loop
        # https://docs.aiohttp.org/en/stable/client_reference.html#baseconnector
        self.conn = aiohttp.TCPConnector(**kwargs)
        self.session: aiohttp.ClientSession = aiohttp.ClientSession(connector=self.conn)
        self.admin_url = admin_url
        self.retry_times = retry_times
        self.retry_interval = retry_interval
        self.headers = {"XXL-JOB-ACCESS-TOKEN": token} if token else {}

    async def registry(self, key, value):
        payload = dict(registryGroup="EXECUTOR", registryKey=key, registryValue=value)
        await self._post("/registry", payload)
        logger.debug("registry successful. %s" % payload)

    async def registryRemove(self, key, value):
        payload = dict(registryGroup="EXECUTOR", registryKey=key, registryValue=value)
        await self._post("/registryRemove", payload)
        logger.info("registryRemove successful. %s" % payload)

    async def callback(self, log_id: int, timestamp: int, code: int = 200, msg: str = None):
        payload = [{
            "logId": log_id,
            "logDateTim": timestamp,
            "handleCode": code,
            "handleMsg": msg,
        }]
        await self._post("/callback", payload)
        logger.debug("callback successful. %s" % payload)

    async def _post(self, path: str, payload: dict) -> Response:
        times = 1
        while times <= self.retry_times or self.retry_times == 0:
            try:
                async with self.session.post(self.admin_url + path, json=payload, headers=self.headers) as response:
                    if response.status == 200:
                        r = Response(**(await response.json()))
                        if not r.ok:
                            raise XXLRegisterError(r.msg)
                        return r
                    raise XXLRegisterError(await response.text())
            except aiohttp.ClientConnectionError as e:
                logger.error(f'Connection error {times} times: {str(e)}, retry afert {self.retry_interval}')
                await asyncio.sleep(self.retry_interval)
                times += 1
        raise ClientError('Connection error, retry times {}'.format(times))

    async def close(self):
        await self.session.close()
        logger.info("http session is closed.")
