xxl-jobs 的python客户端实现
=============================

.. image:: https://img.shields.io/pypi/v/pyxxl?color=%2334D058&label=pypi%20package
    :target: https://pypi.org/project/pyxxl

.. image:: https://img.shields.io/pypi/pyversions/pyxxl.svg?color=%2334D058
    :target: https://pypi.org/project/pyxxl

.. image:: https://img.shields.io/codecov/c/github/fcfangcc/pyxxl?color=%2334D058
    :target: https://codecov.io/gh/fcfangcc/pyxxl


使用pyxxl可以方便的把Python写的方法注册到xxl-job中,使用xxl-job-admin管理Python定时任务和周期任务

如何使用
=======================
.. code:: shell

    pip install pyxxl

具体可以查看example文件夹下面的2个例子

.. code:: python

    import logging
    import asyncio

    from pyxxl import PyxxlRunner, JobHandler

    logger = logging.getLogger("pyxxl")
    logger.setLevel(logging.INFO)
    xxl_handler = JobHandler()

    @xxl_handler.register
    async def test_task():
        await asyncio.sleep(30)
        return "成功30"


    @xxl_handler.register(name="xxxxx")
    @xxxxx # 自己定义的装饰器必须放在下面
    async def abc():
        await asyncio.sleep(3)
        return "成功3"


    runner = PyxxlRunner(
        "http://localhost:8080/xxl-job-admin/api/",
        executor_name="xxl-job-executor-sample",
        port=9999,
        host="172.17.0.1",
        handler=xxl_handler,
    )
    runner.run_executor()




开发人员
=======================
下面是开发人员如何快捷的搭建开发调试环境

=====================
启动xxl的调度中心
=====================

.. code:: shell

    ./init_dev_env.sh

=====================
启动执行器
=====================
.. code:: shell

    poetry install
    # 修改app.py中相关的配置信息,然后启动
    poetry run python example/app.py


======================
TODOs
======================

- [x] 自定义查看日志函数
- [x] docs
