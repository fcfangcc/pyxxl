from typing import Any, Callable, TypedDict, TypeVar

Handler0 = Callable[[Any, Any], Any]
Handler1 = Callable[..., Any]

DecoratedCallable = TypeVar("DecoratedCallable", Handler0, Handler1)


class LogResponse(TypedDict):
    fromLineNum: int
    toLineNum: int
    logContent: str
    isEnd: bool


class LogRequest(TypedDict):
    logDateTim: int
    logId: int
    fromLineNum: int  # min 1
