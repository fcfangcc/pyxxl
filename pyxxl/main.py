import asyncio
import logging

from multiprocessing import Process
from typing import AsyncGenerator, Optional

from aiohttp import web

from pyxxl.execute import Executor, JobHandler
from pyxxl.service import create_app
from pyxxl.utils import ensure_host
from pyxxl.xxl_client import XXL


logger = logging.getLogger("pyxxl.run")


class PyxxlRunner:
    xxl_client: Optional[XXL] = None
    executor: Optional[Executor] = None
    register_task: Optional[asyncio.Task] = None
    daemon: Optional[Process] = None

    def __init__(
        self,
        xxl_admin_baseurl: str,
        executor_name: str,
        handler: Optional[JobHandler] = None,
        access_token: Optional[str] = None,
        host: Optional[str] = None,
        port: int = 9999,
    ):
        """
        Example:
            ```
            runner = PyxxlRunner(
                "http://localhost:8080/xxl-job-admin/api/",
                executor_name="xxl-job-executor-sample",
                port=9999,
                host="172.17.0.1",
                handler=xxl_handler,
            )
            ```


        Args:
            xxl_admin_baseurl (str): xxl-admin服务端暴露的restful接口url(如http://localhost:8080/xxl-job-admin/api/)
            executor_name (str): xxl-admin上定义的执行器名称,必须一致否则无法注册(如xxl-job-executor-sample)
            handler (JobHandler, optional): 执行器支持的job,没有预先定义的job名称也会执行失败
            access_token (str, optional): xxl-admin的认证token,如果没有开启不需要传. Defaults to None.
            host (_type_, optional): 执行器绑定的host,xxl-admin通过这个host来回调pyxxl执行器,如果不填会默认取第一个网卡的地址. Defaults to None.
            port (int, optional): 执行器绑定的http服务的端口,作用同host. Defaults to 9999.
        """
        self.host = ensure_host(host)
        self.port = port
        self.xxl_admin_baseurl = xxl_admin_baseurl
        self.executor_name = executor_name
        self.executor_baseurl = "http://{host}:{port}".format(host=self.host, port=self.port)
        self.access_token = access_token
        self.handler = handler or JobHandler()

    async def _register_task(self, xxl_client: XXL) -> None:
        # todo: 这是个调度器的bug，必须循环去注册，不然会显示为离线
        # https://github.com/xuxueli/xxl-job/issues/2090
        while True:
            await xxl_client.registry(self.executor_name, self.executor_baseurl)
            await asyncio.sleep(10)

    def _get_xxl_clint(self) -> XXL:
        """for moke"""
        return XXL(self.xxl_admin_baseurl, token=self.access_token)

    async def _init(self) -> None:
        self.xxl_client = self._get_xxl_clint()
        self.executor = Executor(self.xxl_client, handler=self.handler)
        self.register_task = asyncio.create_task(self._register_task(self.xxl_client), name="pyxxl-register")

    async def _cleanup_ctx(self, app: web.Application) -> AsyncGenerator:
        await self._init()
        app["xxl_client"] = self.xxl_client
        app["executor"] = self.executor
        app["register_task"] = self.register_task
        if self.executor and self.executor.handler:
            logger.info("register with handlers %s", list(self.executor.handler.handlers()))
        else:
            logger.warning("register with handlers is empty")

        yield

        app["register_task"].cancel()
        await app["xxl_client"].registryRemove(self.executor_name, self.executor_baseurl)
        await app["executor"].shutdown()
        await app["xxl_client"].close()
        logger.info("cleanup executor success.")

    def create_server_app(self) -> web.Application:
        app = create_app()
        app.cleanup_ctx.append(self._cleanup_ctx)
        return app

    def run_executor(self, handle_signals: bool = True) -> None:
        web.run_app(
            self.create_server_app(),
            port=self.port,
            host=self.host,
            handle_signals=handle_signals,
        )

    def run_with_daemon(self) -> None:
        def _runner() -> None:
            self.run_executor(handle_signals=True)

        daemon = Process(target=_runner, name="pyxxl", daemon=True)
        daemon.start()
        self.daemon = daemon

    # def exit_daemon(self):
    #     logger.info("Exit daemon name=%s", self.daemon.name )
    #     self.daemon.terminate()
