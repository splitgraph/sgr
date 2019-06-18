"""
Public API for Splitgraph
"""

from splitgraph.core.registry import *

from .config import CONFIG
from .core.repository import Repository
from .engine import get_engine, switch_engine
from .exceptions import SplitGraphError
