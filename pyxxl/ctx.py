from contextvars import ContextVar
from pyxxl.schema import RunData

_global_vars = ContextVar('pyxxl_vars', default=None)


class GlobalVars:

    @staticmethod
    def _set_var(name, obj):
        if _global_vars.get() is None:
            _global_vars.set({})
        _global_vars.get()[name] = obj

    @staticmethod
    def _get_var(name):
        return _global_vars.get()[name]

    @staticmethod
    def set_xxl_run_data(data: RunData):
        GlobalVars._set_var('xxl_kwargs', data)

    @property
    def xxl_run_data(self) -> RunData:
        return self._get_var('xxl_kwargs')


g = GlobalVars()
