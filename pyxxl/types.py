from typing import Any, Callable, TypeVar


Handler0 = Callable[[Any, Any], Any]
Handler1 = Callable[..., Any]

DecoratedCallable = TypeVar("DecoratedCallable", Handler0, Handler1)
