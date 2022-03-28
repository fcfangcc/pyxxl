from multiprocessing import Process
import logging
import asyncio
from aiohttp import web
from pyxxl.xxl_client import XXL
from pyxxl.execute import Executor
from pyxxl.utils import ensure_host
from pyxxl.service import create_app
from pyxxl.register import HANDLERS

logger = logging.getLogger("pyxxl.run")


class PyxxlRunner:
    xxl_client: XXL = None
    executor: Executor = None
    register_task = None
    daemon = None

    def __init__(
        self,
        xxl_admin_baseurl,
        executor_name: str,
        access_token: str = None,
        host=None,
        port=9999,
        handlers=None,
    ):
        self.host = ensure_host(host)
        self.port = port
        self.xxl_admin_baseurl = xxl_admin_baseurl
        self.executor_name = executor_name
        self.executor_baseurl = "http://{host}:{port}".format(host=self.host, port=self.port)
        self.access_token = access_token
        self.handlers = handlers or HANDLERS

    async def _register_task(self, xxl_client: XXL):
        while True:
            await xxl_client.registry(self.executor_name, self.executor_baseurl)
            await asyncio.sleep(5)

    async def _on_startup(self):
        self.xxl_client = XXL(self.xxl_admin_baseurl, token=self.access_token)
        self.executor = Executor(self.xxl_client, handlers=self.handlers)
        self.register_task = asyncio.create_task(self._register_task(self.xxl_client), name="pyxxl-register")

    # pylint: disable=unused-argument
    async def _on_cleanup(self, *args, **kwargs):
        self.register_task.cancel()
        logger.info("unregister executor success.")
        await self.xxl_client.registryRemove(self.executor_name, self.executor_baseurl)
        await self.executor.shutdown()
        await self.xxl_client.close()
        logger.info("cleanup executor success.")

    async def web_on_startup(self, app: web.Application):
        await self._on_startup()
        app["xxl_client"] = self.xxl_client
        app["executor"] = self.executor
        app["register_task"] = self.register_task
        logger.info("init executor web server.")
        logger.info("register with handlers %s", list(HANDLERS.keys()))

    def on_cleanup(self, loop=None):
        loop = loop or asyncio.get_event_loop()
        logger.info("start close pyxxl with loop %s", loop)
        task = asyncio.run_coroutine_threadsafe(self._on_cleanup(), loop)
        task.result()

    def run_executor(self, handle_signals=True):
        app = create_app()
        app.on_startup.append(self.web_on_startup)
        app.on_cleanup.append(self._on_cleanup)
        web.run_app(app, port=self.port, host=self.host, handle_signals=handle_signals)

    def run_with_daemon(self):

        def _runner():
            self.run_executor(handle_signals=True)

        daemon = Process(target=_runner, name="pyxxl", daemon=True)
        daemon.start()
        self.daemon = daemon

    # def exit_daemon(self):
    #     logger.info("Exit daemon name=%s", self.daemon.name )
    #     self.daemon.terminate()
