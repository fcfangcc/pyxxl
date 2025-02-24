import asyncio
import logging
import os
from multiprocessing import Process
from typing import Any, AsyncGenerator, NamedTuple, Optional

from aiohttp import web

from pyxxl import executor
from pyxxl.logger import DiskLog, LogBase, RedisLog
from pyxxl.server import create_app
from pyxxl.setting import ExecutorConfig
from pyxxl.utils import setup_logging, try_import
from pyxxl.xxl_client import XXL

if try_import("prometheus_client"):
    from pyxxl import prometheus

    class Executor(executor.Executor):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._successed_callback = prometheus.success
            self._failed_callback = prometheus.failed

else:

    class Executor(executor.Executor): ...  # type: ignore[no-redef]


async def server_info_ctx(app: web.Application) -> AsyncGenerator:
    pid = os.getpid()
    state: State = app["pyxxl_state"]
    state.executor_logger.info(f"start executor server with pid {pid}.")
    yield
    state.executor_logger.info(f"stop executor server. pid={pid}.")


class State(NamedTuple):
    xxl_client: XXL
    executor: Executor
    task_log: LogBase
    executor_logger: logging.Logger


class PyxxlRunner:
    daemon: Optional[Process] = None
    _logging_setup: bool = False

    def __init__(
        self,
        config: ExecutorConfig,
        handler: Optional[executor.JobHandler] = None,
    ):
        """
        !!! example

            ```python
            runner = PyxxlRunner(
                ExecutorConfig(
                    xxl_admin_baseurl="http://localhost:8080/xxl-job-admin/api/",
                    executor_app_name="xxl-job-executor-sample",
                    executor_url="http://172.17.0.1:9999",
                    )
                ,
                handler=xxl_handler,
            )
            ```
        Args:
            config (ExecutorConfig): 配置参数
            handler (JobHandler, optional): 执行器支持的job,没有预先定义的job名称也会执行失败
        """

        self.handler = handler or executor.JobHandler(logger=config.executor_logger)
        self.config = config
        self.log_level = logging.DEBUG if self.config.debug else logging.INFO

    async def _register_task(self, xxl_client: XXL) -> None:
        # todo: 这是个调度器的bug，必须循环去注册，不然会显示为离线
        # https://github.com/xuxueli/xxl-job/issues/2090
        try:
            while True:
                await xxl_client.registry(self.config.executor_app_name, self.config.executor_baseurl)
                await asyncio.sleep(10)
        finally:
            self.config.executor_logger.warning("Register task is exit.")

    def _get_xxl_clint(self) -> XXL:
        """for moke"""
        return XXL(self.config.xxl_admin_baseurl, token=self.config.access_token, logger=self.config.executor_logger)

    def _get_log(self) -> LogBase:
        if self.config.log_target == "disk":
            return DiskLog(
                log_path=self.config.log_local_dir,
                expired_days=self.config.log_expired_days,
                logger=self.config.executor_logger,
            )

        if self.config.log_target == "redis":
            return RedisLog(
                self.config.executor_app_name,
                self.config.log_redis_uri,
                expired_days=self.config.log_expired_days,
                logger=self.config.executor_logger,
            )

        raise NotImplementedError

    async def _cleanup_ctx(self, app: web.Application) -> AsyncGenerator:
        task_log = self._get_log()
        xxl_client = self._get_xxl_clint()
        executor = Executor(
            xxl_client,
            config=self.config,
            handler=self.handler,
            logger_factory=task_log,
        )

        state = State(
            xxl_client=xxl_client,
            executor=executor,
            task_log=task_log,
            executor_logger=self.config.executor_logger,
        )
        app["pyxxl_state"] = state
        executor_log_task = asyncio.create_task(state.task_log.expired_loop(), name="log_task")
        register_task = asyncio.create_task(self._register_task(state.xxl_client), name="register_task")
        if state.executor.handler:
            state.executor_logger.info("register with handlers %s", list(executor.handler.handlers_info()))
        else:
            state.executor_logger.warning("register with handlers is empty")  # pragma: no cover

        yield

        register_task.cancel()
        executor_log_task.cancel()
        await state.xxl_client.registryRemove(self.config.executor_app_name, self.config.executor_baseurl)
        if self.config.graceful_close:
            await state.executor.graceful_close(self.config.graceful_timeout)
        else:
            await state.executor.shutdown()
        await state.xxl_client.close()
        state.executor_logger.info("cleanup executor success.")

    def create_server_app(self) -> web.Application:
        """获取执行器的app对象,可以使用自己喜欢的服务器启动这个webapp"""
        app = create_app()
        app.cleanup_ctx.append(self._cleanup_ctx)
        app.cleanup_ctx.append(server_info_ctx)
        return app

    def _setup_logging(self) -> None:
        if not self._logging_setup:
            setup_logging(self.config.executor_log_path, "pyxxl", level=self.log_level)

    def run_executor(self, handle_signals: bool = True) -> None:
        """用aiohttp的web服务器启动执行器"""
        self._setup_logging()
        web.run_app(
            self.create_server_app(),
            port=self.config.executor_listen_port,
            host=self.config.executor_listen_host,
            handle_signals=handle_signals,
        )

    def _runner(self) -> None:
        self.run_executor(handle_signals=True)

    def run_with_daemon(self) -> None:
        """新开一个进程以后台方式运行,一般和gunicorn一起使用"""

        daemon = Process(target=self._runner, name="pyxxljob", daemon=True)
        daemon.start()
        self.daemon = daemon

    @property
    def register(self) -> Any:
        return self.handler.register

    # def exit_daemon(self):
    #     logger.info("Exit daemon name=%s", self.daemon.name )
    #     self.daemon.terminate()
