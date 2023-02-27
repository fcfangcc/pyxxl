import logging
import threading
from contextvars import ContextVar
from typing import Any, Optional

from pyxxl.schema import RunData

_global_vars: ContextVar[dict] = ContextVar("pyxxl_vars", default={})


class GlobalVars:
    @staticmethod
    def _set_var(name: str, obj: Any) -> None:
        _global_vars.get()[name] = obj

    @staticmethod
    def _get_var(name: str) -> Any:
        return _global_vars.get()[name]

    @staticmethod
    def try_get(name: str) -> Optional[Any]:
        return _global_vars.get().get(name)

    @staticmethod
    def set_xxl_run_data(data: RunData) -> None:
        GlobalVars._set_var("xxl_kwargs", data)

    @property
    def xxl_run_data(self) -> RunData:
        return self._get_var("xxl_kwargs")

    @staticmethod
    def set_task_logger(logger: logging.Logger) -> None:
        GlobalVars._set_var("task_logger", logger)

    @property
    def logger(self) -> logging.Logger:  # pragma: no cover
        return self._get_var("task_logger")

    @staticmethod
    def set_cancel_event(event: threading.Event) -> None:
        GlobalVars._set_var("cancel_event", event)

    @property
    def cancel_event(self) -> threading.Event:  # pragma: no cover
        return self._get_var("cancel_event")


g = GlobalVars()
