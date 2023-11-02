import inspect
import logging
import os
from dataclasses import asdict, dataclass, field
from typing import Literal, Optional

from yarl import URL

from pyxxl.utils import get_network_ip, setup_logging

logger = logging.getLogger(__name__)


@dataclass
class ExecutorConfig:
    """
    如果安装了python-dotenv,支持从.env文件加载配置项

    !!! warning

        会从环境变量覆盖配置，列如access_token参数

        优先级为: 环境变量access_token > 环境变量ACCESS_TOKEN > ExecutorConfig().access_token

    """

    xxl_admin_baseurl: str
    """xxl-admin服务端暴露的restful接口url(如http://localhost:8080/xxl-job-admin/api/). 必填"""
    executor_app_name: str
    """xxl-admin上定义的执行器名称,必须一致否则无法注册(如xxl-job-executor-sample). 必填"""
    access_token: Optional[str] = None
    """调度器的token. Default: None"""

    executor_log_path: str = "pyxxl.log"
    """执行器日志输出的路径(注意路径必须存在). Default: pyxxl.log"""
    executor_port: int = 9999
    """执行器绑定的http服务的端口,作用同host. Default: 9999"""
    executor_host: str = field(default_factory=get_network_ip)
    """执行器绑定的host,xxl-admin通过这个host来回调pyxxl执行器,如果不填会默认取第一个网卡的地址. Default: 获取到第一个网卡的ip地址"""
    executor_listen_port: int = 0
    """Default: executor_port"""
    executor_listen_host: str = ""
    """
    执行器HTTP服务绑定的HOST,大部分情况下不需要设置. Default: executor_host

    当执行器通过了端口转发暴露给admin的时候,需要把executor_host填写为直连admin的地址.

    列如调用路径为 xxl-admin -> nginx_ip_or_domain:80 -> executor:9999
    这个时候需要配置为

        executor_port=80
        executor_host=nginx_ip_or_domain
        executor_listen_port=9999
        executor_listen_host="0.0.0.0"

    """

    max_workers: int = 30
    """执行器线程池（执行同步任务时使用）. Default: 30"""
    task_timeout: int = 60 * 10
    """任务的默认超时时间,如果调度器传了以参数executorTimeout为准. Default: 60 * 10"""
    task_queue_length: int = 30
    """任务的队列长度.单机串行的队列长度,当阻塞的任务大于此值时会抛弃. Default: 30"""
    graceful_close: bool = False
    """是否优雅关闭. Default: True"""
    graceful_timeout: int = 60 * 5
    """优雅关闭的等待时间,超过改时间强制停止任务. Default: 60 * 5"""

    log_target: Literal["disk", "redis"] = "disk"
    """任务日志存储的地方.  Default: disk"""
    log_local_dir: str = "logs"
    """任务日志存储的本地目录,默认为当前目录logs文件夹"""
    log_redis_uri: str = ""
    """任务日志存储到redis的连接地址"""
    log_expired_days: int = 14
    """任务日志存储的本地的过期天数. Default: 14"""

    dotenv_try: bool = True
    dotenv_path: Optional[str] = None
    """.env文件的路径,默认为当前路径下的.env文件."""
    debug: bool = False

    def __post_init__(self) -> None:
        if self.debug:
            setup_logging(self.executor_log_path, __name__, level=logging.DEBUG)

        if self.dotenv_try:
            self._try_load_from_dotenv()

        self._valid_xxl_admin_baseurl()
        self._valid_executor_app_name()
        self._valid_logger_target()

        if not self.executor_listen_host:
            self.executor_listen_host = self.executor_host

        if not self.executor_listen_port:
            self.executor_listen_port = self.executor_port

        logger.debug("init config: %s", asdict(self))

    def _try_load_from_dotenv(self) -> None:
        try:
            from dotenv import load_dotenv

            load_dotenv(self.dotenv_path)
        except ImportError:  # pragma: no cover
            pass

        for param in inspect.signature(ExecutorConfig).parameters.values():
            env_val = os.getenv(param.name) or os.getenv(param.name.upper())
            if env_val is not None:
                logger.debug("Get [%s] config from env: [%s]" % (param.name, env_val))
                if param.annotation is bool:
                    real_value = env_val in ["true", "True"]
                else:
                    real_value = param.annotation(env_val)
                setattr(self, param.name, real_value)

    def _valid_xxl_admin_baseurl(self) -> None:
        _admin_url: URL = URL(self.xxl_admin_baseurl)
        if not (_admin_url.scheme.startswith("http") and _admin_url.path.endswith("/")):
            raise ValueError("admin_url must like http://localhost:8080/xxl-job-admin/api/")

    def _valid_executor_app_name(self) -> None:
        if not self.executor_app_name:
            raise ValueError("executor_app_name is required.")

    def _valid_logger_target(self) -> None:
        if self.log_target == "disk" and not self.log_local_dir:
            raise ValueError("log_target 'disk' config item 'log_local_dir' is necessary.")

        if self.log_target == "redis" and not self.log_redis_uri:
            raise ValueError("log_target 'redis' config item 'log_redis_uri' is necessary.")

    @property
    def executor_baseurl(self) -> str:
        """暴露给xxl-admin的地址"""
        return "http://{host}:{port}".format(host=self.executor_host, port=self.executor_port)
