import logging
import threading
from contextvars import ContextVar
from typing import Any, Optional

from pyxxl.schema import RunData


class GlobalVars:
    _DATA: ContextVar = ContextVar("_DATA")
    _LOGGER: ContextVar = ContextVar("_LOGGER")
    _EVENT: ContextVar = ContextVar("_EVENT")

    @classmethod
    def set_xxl_run_data(cls, data: RunData) -> None:
        cls._DATA.set(data)

    @classmethod
    def try_get_data(cls) -> Optional[Any]:
        return cls._DATA.get(None)

    @property
    def xxl_run_data(self) -> RunData:
        return self._DATA.get()

    @classmethod
    def set_task_logger(cls, logger: logging.Logger) -> None:
        cls._LOGGER.set(logger)

    @property
    def logger(self) -> logging.Logger:  # pragma: no cover
        return self._LOGGER.get()

    @classmethod
    def set_cancel_event(cls, event: threading.Event) -> None:
        cls._EVENT.set(event)

    @property
    def cancel_event(self) -> threading.Event:  # pragma: no cover
        return self._EVENT.get()


g = GlobalVars()
