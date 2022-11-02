import asyncio
import logging

from multiprocessing import Process
from typing import AsyncGenerator, Optional

from aiohttp import web

from pyxxl.executor import Executor, JobHandler
from pyxxl.server import create_app
from pyxxl.setting import ExecutorConfig
from pyxxl.xxl_client import XXL


logger = logging.getLogger(__name__)


class PyxxlRunner:
    xxl_client: Optional[XXL] = None
    executor: Optional[Executor] = None
    register_task: Optional[asyncio.Task] = None
    daemon: Optional[Process] = None

    def __init__(
        self,
        config: ExecutorConfig,
        handler: Optional[JobHandler] = None,
    ):
        """
        !!! example

            ```python
            runner = PyxxlRunner(
                ExecutorConfig(
                    xxl_admin_baseurl="http://localhost:8080/xxl-job-admin/api/",
                    executor_app_name="xxl-job-executor-sample",
                    executor_host="172.17.0.1"
                    )
                ,
                handler=xxl_handler,
            )
            ```
        Args:
            config (ExecutorConfig): 配置参数 [ExecutorConfig](/apis/config)
            handler (JobHandler, optional): 执行器支持的job,没有预先定义的job名称也会执行失败
        """

        self.handler = handler or JobHandler()
        self.config = config

    async def _register_task(self, xxl_client: XXL) -> None:
        # todo: 这是个调度器的bug，必须循环去注册，不然会显示为离线
        # https://github.com/xuxueli/xxl-job/issues/2090
        while True:
            await xxl_client.registry(self.config.executor_app_name, self.config.executor_baseurl)
            await asyncio.sleep(10)

    def _get_xxl_clint(self) -> XXL:
        """for moke"""
        return XXL(self.config.xxl_admin_baseurl, token=self.config.access_token)

    async def _init(self) -> None:
        self.xxl_client = self._get_xxl_clint()
        self.executor = Executor(self.xxl_client, config=self.config, handler=self.handler)
        self.register_task = asyncio.create_task(self._register_task(self.xxl_client), name="pyxxl-register")

    async def _cleanup_ctx(self, app: web.Application) -> AsyncGenerator:
        await self._init()
        app["xxl_client"] = self.xxl_client
        app["executor"] = self.executor
        app["register_task"] = self.register_task
        if self.executor and self.executor.handler:
            logger.info("register with handlers %s", list(self.executor.handler.handlers_info()))
        else:
            logger.warning("register with handlers is empty")

        yield

        app["register_task"].cancel()
        await app["xxl_client"].registryRemove(self.config.executor_app_name, self.config.executor_baseurl)
        if self.config.graceful_close:
            await app["executor"].graceful_close(self.config.graceful_timeout)
        else:
            await app["executor"].shutdown()
        await app["xxl_client"].close()
        logger.info("cleanup executor success.")

    def create_server_app(self) -> web.Application:
        """获取执行器的app对象,可以使用自己喜欢的服务器启动这个webapp"""
        app = create_app()
        app.cleanup_ctx.append(self._cleanup_ctx)
        return app

    def run_executor(self, handle_signals: bool = True) -> None:
        """用aiohttp的web服务器启动执行器"""
        web.run_app(
            self.create_server_app(),
            port=self.config.executor_port,
            host=self.config.executor_host,
            handle_signals=handle_signals,
        )

    def _runner(self) -> None:
        self.run_executor(handle_signals=True)

    def run_with_daemon(self) -> None:
        """新开一个进程以后台方式运行,一般和gunicorn一起使用"""

        daemon = Process(target=self._runner, name="pyxxljob", daemon=True)
        daemon.start()
        self.daemon = daemon

    # def exit_daemon(self):
    #     logger.info("Exit daemon name=%s", self.daemon.name )
    #     self.daemon.terminate()
