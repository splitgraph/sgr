"""
Public API for Splitgraph
"""
import logging

from ._data.registry import publish_tag, unpublish_repository, get_published_info
from .commands import *
from .connection import get_connection, serialize_connection_string
from .exceptions import SplitGraphException
from .pg_utils import get_all_tables
from .splitfile.execution import *

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
