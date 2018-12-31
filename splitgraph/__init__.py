"""
Public API for Splitgraph
"""
import logging

from splitgraph.core.registry import *
from .config import CONFIG
from .core.repository import Repository
from .engine import get_engine, switch_engine
from .exceptions import SplitGraphException
from .splitfile import *

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
