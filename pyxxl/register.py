from typing import Dict
import asyncio
import logging
import functools
from pyxxl.schema import HandlerInfo
from pyxxl.error import JobRegisterError

logger = logging.getLogger("pyxxl.register")

HANDLERS: Dict[str, HandlerInfo] = {}


def job_hander(*args, name=None, replace=False):
    """将函数注册到可执行的job中,如果其他地方要调用该方法，replace修改为True"""

    def func_wrapper(func):
        handler_name = name or func.__name__
        if handler_name in HANDLERS and replace is False:
            raise JobRegisterError("handler %s already registered." % handler_name)
        HANDLERS[handler_name] = HandlerInfo(handler=func)
        logger.info("register job %s,is async: %s" % (handler_name, asyncio.iscoroutinefunction(func)))

        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def inner_wrapper(*args, **kwargs):
                return await func(*args, **kwargs)
        else:

            @functools.wraps(func)
            def inner_wrapper(*args, **kwargs):
                return func(*args, **kwargs)

        return inner_wrapper

    if len(args) == 1:
        return func_wrapper(args[0])

    return func_wrapper
