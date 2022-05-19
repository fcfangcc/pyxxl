import importlib.metadata

from .execute import JobHandler
from .main import PyxxlRunner


__version__ = importlib.metadata.version("pyxxl")
