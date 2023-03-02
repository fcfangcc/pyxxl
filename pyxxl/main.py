import asyncio
import functools
import logging
from multiprocessing import Process
from typing import AsyncGenerator, Optional

from aiohttp import web

from pyxxl.executor import Executor, JobHandler
from pyxxl.logger import DiskLog, LogBase, RedisLog
from pyxxl.server import create_app
from pyxxl.setting import ExecutorConfig
from pyxxl.utils import setup_logging, try_import
from pyxxl.xxl_client import XXL

logger = logging.getLogger(__name__)

if try_import("prometheus_client"):
    from pyxxl import prometheus

    Executor: Executor = functools.partial(  # type: ignore[no-redef]
        Executor,
        successd_callback=prometheus.success,
        failed_callback=prometheus.failed,
    )


class PyxxlRunner:
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
        log_level = logging.DEBUG if self.config.debug else logging.INFO
        setup_logging(self.config.executor_log_path, level=log_level)

    async def _register_task(self, xxl_client: XXL) -> None:
        # todo: 这是个调度器的bug，必须循环去注册，不然会显示为离线
        # https://github.com/xuxueli/xxl-job/issues/2090
        while True:
            await xxl_client.registry(self.config.executor_app_name, self.config.executor_baseurl)
            await asyncio.sleep(10)

    def _get_xxl_clint(self) -> XXL:
        """for moke"""
        return XXL(self.config.xxl_admin_baseurl, token=self.config.access_token)

    def _get_log(self) -> LogBase:
        if self.config.log_target == "disk":
            return DiskLog(log_path=self.config.log_local_dir, expired_days=self.config.log_expired_days)

        if self.config.log_target == "redis":
            return RedisLog(
                self.config.executor_app_name, self.config.log_redis_uri, expired_days=self.config.log_expired_days
            )

        raise NotImplementedError

    async def _cleanup_ctx(self, app: web.Application) -> AsyncGenerator:
        # setup task log
        executor_log = self._get_log()
        executor_log_task = asyncio.create_task(executor_log.expired_loop(), name="log_task")

        xxl_client = self._get_xxl_clint()
        executor = Executor(xxl_client, config=self.config, handler=self.handler, logger_factory=executor_log)
        register_task = asyncio.create_task(self._register_task(xxl_client), name="register_task")

        app["xxl_client"] = xxl_client
        app["executor"] = executor
        app["executor_log"] = executor_log

        if executor.handler:
            logger.info("register with handlers %s", list(executor.handler.handlers_info()))
        else:
            logger.warning("register with handlers is empty")  # pragma: no cover

        yield

        register_task.cancel()
        executor_log_task.cancel()
        await xxl_client.registryRemove(self.config.executor_app_name, self.config.executor_baseurl)
        if self.config.graceful_close:
            await executor.graceful_close(self.config.graceful_timeout)
        else:
            await executor.shutdown()
        await xxl_client.close()
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
