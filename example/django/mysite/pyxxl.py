from exampleapp.models import User

from pyxxl import ExecutorConfig, PyxxlRunner
from pyxxl.ctx import g

# This should get config from django settings
config = ExecutorConfig(
    xxl_admin_baseurl="http://localhost:8080/xxl-job-admin/api/",
    executor_app_name="xxl-job-executor-sample",
    executor_listen_host="127.0.0.1",  # xxl-admin监听时绑定的host,默认为第一个网卡地址
    debug=True,
)

pyxxl_app = PyxxlRunner(config)


@pyxxl_app.register(name="django_test1")
def django_test1():
    g.logger.info("Job %s get executor params: %s" % (g.xxl_run_data.jobId, g.xxl_run_data.executorParams))
    return "成功3"


@pyxxl_app.register(name="django_test2")
def django_test2():
    users = User.objects.all()
    return ",".join([user.name for user in users])
