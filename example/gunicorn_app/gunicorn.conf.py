import atexit
from multiprocessing.util import _exit_function
from pyxxl import PyxxlRunner

bind = ["0.0.0.0:8000"]
backlog = 512
workers = 1
timeout = 300
graceful_timeout = 2
limit_request_field_size = 8192


def when_ready(server):
    # pylint: disable=import-outside-toplevel,unused-import,no-name-in-module
    from app import xxl_handler
    atexit.unregister(_exit_function)
    runner = PyxxlRunner(
        "http://localhost:8080/xxl-job-admin/api/",
        executor_name="xxl-job-executor-sample",
        host="172.17.0.1",
        handler=xxl_handler,
    )
    server.pyxxl_runner = runner
    runner.run_with_daemon()
