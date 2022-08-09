import importlib.metadata

from .executor import JobHandler
from .main import PyxxlRunner
from .setting import ExecutorConfig


__version__ = importlib.metadata.version("pyxxl")
