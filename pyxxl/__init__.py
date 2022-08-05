import importlib.metadata

from .executor import JobHandler
from .main import PyxxlRunner


__version__ = importlib.metadata.version("pyxxl")
