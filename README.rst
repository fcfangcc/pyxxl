xxl-jobs 的python客户端实现
=============================
使用pyxxl可以方便的把Python写的方法注册到xxl-job中，使用xxl-job-admin管理Python定时任务和周期任务

如何使用
=======================
.. code:: shell

    pip install pyxxl

.. code:: python

    import logging
    import asyncio

    from pyxxl import run_executor, job_hander

    logger = logging.getLogger("pyxxl")
    logger.setLevel(logging.DEBUG)


    @job_hander
    async def test_task():
        await asyncio.sleep(30)
        return "成功30"


    @job_hander(name="xxxxx")
    @xxxxx # 自己定义的装饰器必须放在下面
    async def abc():
        await asyncio.sleep(3)
        return "成功3"


    runner = PyxxlRunner(
        "http://localhost:8080/xxl-job-admin/api/",
        "test",
        port=9999,
        host="172.17.0.1",
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
.. code:: python

    python run.py


======================
TODOs
======================

- [x] 自定义查看日志函数