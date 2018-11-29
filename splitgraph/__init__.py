"""
Public API for Splitgraph
"""
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

# Still not sure how to split these up.
from .connection import get_connection, serialize_connection_string
from .exceptions import SplitGraphException
from .commands import *
from .splitfile.execution import *
from .pg_utils import get_all_tables
from .drawing import render_tree
