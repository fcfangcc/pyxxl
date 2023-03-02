# xxl-jobs 的python客户端实现

<p align="center">
<a href="https://pypi.org/project/pyxxl" target="_blank">
    <img src="https://img.shields.io/pypi/v/pyxxl?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
<a href="https://pypi.org/project/pyxxl" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/pyxxl.svg?color=%2334D058" alt="Supported Python versions">
</a>
<a href="https://pypi.org/project/pyxxl" target="_blank">
    <img src="https://img.shields.io/codecov/c/github/fcfangcc/pyxxl?color=%2334D058" alt="Coverage">
</a>
</p>

使用pyxxl可以方便的把Python写的方法注册到[XXL-JOB](https://github.com/xuxueli/xxl-job)中,使用XXL-JOB-ADMIN管理Python定时任务和周期任务

实现原理：通过XXL-JOB提供的RESTful API接口进行对接

<font color="#dd0000">注意！！！如果用同步的方法，请查看下面同步任务注意事项。</font>

## 已经支持的功能

* 执行器注册到job-admin
* task注册，类似于flask路由装饰器的用法
* 任务的管理（支持在界面上取消，发起等操作，任务完成后会回调admin）
* 所有阻塞策略的支持
* 异步支持（推荐）
* job-admin上查看日志

## 适配的XXL-JOB版本

* XXL-JOB:2.3.0

如遇到不兼容的情况请issue告诉我XXL-JOB版本我会尽量适配

## 如何使用

```shell
pip install pyxxl
# 如果日志需要写入redis
pip install pyxxl[redis]
# 如果需要从.env加载配置
pip install pyxxl[dotenv]
# 安装所有功能
pip install pyxxl[all]
```

```python
import asyncio

from pyxxl import ExecutorConfig, PyxxlRunner

config = ExecutorConfig(
    xxl_admin_baseurl="http://localhost:8080/xxl-job-admin/api/",
    executor_app_name="xxl-job-executor-sample",
    executor_host="172.17.0.1",
)

app = PyxxlRunner(config)

@app.handler.register(name="demoJobHandler")
async def test_task():
    await asyncio.sleep(5)
    return "成功..."

# 如果你代码里面没有实现全异步，请使用同步函数，不然会阻塞其他任务
@app.handler.register(name="xxxxx")
def test_task3():
    return "成功3"


app.run_executor()
```


更多示例和接口文档请参考 [PYXXL文档](https://fcfangcc.github.io/pyxxl/example/) ，具体代码在example文件夹下面

## 监控指标

```shell
pip install pyxxl[metrics]
```

安装metrics扩展后，执行器会自动加载prometheus的指标监控功能

访问地址为: http://executor_host:port/metrics

## 同步任务注意事项
同步任务会放到线程池中运行，无法正确接受cancel信号和timeout配置

如果需要使用同步任务并且无法控制（预料）里面执行时间，又需要进行超时和cancel功能的，需要接受pyxxl发出的cancel_event，当该cancel_event被设置时需要中断任务，下面是一个案例:

```python
...

@app.handler.register(name="sync_func")
def sync_loop_demo():
    # 如果同步任务里面有循环，为了支持cancel操作，必须每次都判断g.cancel_event.
    task_params_list = []
    while not g.cancel_event.is_set() and task_parasm_list:
        params = task_params_list.pop()
        time.sleep(3)
    return "ok"

# 如下代码会造成线程池里的线程被永远占用，timeout cancel全部不生效
@app.handler.register(name="sync_func2")
def sync_loop_demo2():
    while True:
        time.sleep(3) # 模拟你运行的任务
    return "ok"

```


## 开发人员
下面是开发人员如何快捷的搭建开发调试环境

### 启动xxl的调度中心

```shell
./init_dev_env.sh
```


### 启动执行器


```shell
# if you need. set venv in project.
# poetry config virtualenvs.in-project true
poetry install --all-extras
# 修改app.py中相关的配置信息,然后启动
poetry run python example/app.py
```
