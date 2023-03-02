import asyncio
import logging
from typing import Any, Dict, List, Optional, Union

import aiohttp
from yarl import URL

from pyxxl.error import ClientError, XXLRegisterError

logger = logging.getLogger(__name__)

JsonType = Union[None, int, str, bool, List[Any], Dict[Any, Any]]


class Response:
    def __init__(self, code: int, msg: Optional[str] = None, **kwargs: Any) -> None:
        self.code = code
        self.msg = msg

    @property
    def ok(self) -> bool:
        return self.code == 200


class XXL:
    def __init__(
        self,
        admin_url: str,
        token: Optional[str] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        retry_times: int = 0,
        retry_interval: int = 5,
        session: Optional[aiohttp.ClientSession] = None,
        **kwargs: Any,
    ) -> None:
        self.loop = loop or asyncio.get_event_loop()
        kwargs["loop"] = self.loop

        _admin_url: URL = URL(admin_url)
        if not (_admin_url.scheme.startswith("http") and _admin_url.path.endswith("/")):
            raise ValueError("admin_url must like http://localhost:8080/xxl-job-admin/api/")

        # https://docs.aiohttp.org/en/stable/client_reference.html#baseconnector
        self.url_path = _admin_url.path
        if not session:  # for pytest
            self.conn = aiohttp.TCPConnector(**kwargs)
            session = aiohttp.ClientSession(
                base_url=_admin_url.origin(),
                connector=self.conn,
            )

        self.session = session

        self.retry_times = retry_times
        self.retry_interval = retry_interval
        self.headers = {"XXL-JOB-ACCESS-TOKEN": token} if token else {}

    async def registry(self, key: str, value: str) -> bool:
        payload = dict(registryGroup="EXECUTOR", registryKey=key, registryValue=value)
        try:
            await self._post("registry", payload, retry_times=1)
            # logger.debug("Registry successful. %s" % payload)
            return True
        except XXLRegisterError as e:
            logger.error("Registry executor failed. %s", e.message)
        return False

    async def registryRemove(self, key: str, value: str) -> None:
        payload = dict(registryGroup="EXECUTOR", registryKey=key, registryValue=value)
        await self._post("registryRemove", payload)
        logger.info("RegistryRemove successful. %s" % payload)

    async def callback(self, log_id: int, timestamp: int, code: int = 200, msg: str = None) -> None:
        payload = [
            {
                "logId": log_id,
                "logDateTim": timestamp,
                "handleCode": code,
                "handleMsg": msg,
            }
        ]
        await self._post("callback", payload)
        logger.debug("Callback successful. %s" % payload)

    async def _post(self, path: str, payload: JsonType, retry_times: Optional[int] = None) -> Response:
        times = 1
        retry_times = retry_times or self.retry_times
        while times <= retry_times or retry_times == 0:
            try:
                async with self.session.post(self.url_path + path, json=payload, headers=self.headers) as response:
                    if response.status == 200:
                        r = Response(**(await response.json()))
                        if not r.ok:
                            raise XXLRegisterError(r.msg or "")
                        return r
                    raise XXLRegisterError(await response.text())
            except aiohttp.ClientConnectionError as e:
                logger.error(
                    "Connection error {} times: {}, retry afert {}".format(times, str(e), self.retry_interval)
                )
                await asyncio.sleep(self.retry_interval)
                times += 1
        raise ClientError("Connection error, retry times {}".format(times))

    async def close(self) -> None:
        await self.session.close()
        logger.info("http session is closed.")
