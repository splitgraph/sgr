"""
Defines the interface for a Splitgraph engine (a backing database), including running basic SQL commands,
tracking tables for changes and uploading/downloading tables to other remote engines.

By default, Splitgraph is backed by Postgres: see :mod:`splitgraph.engine.postgres` for an example of how to
implement a different engine.
"""
from enum import Enum


class ResultShape(Enum):
    """Shape that the result of a query will be coerced to"""

    NONE = 0  # No result expected
    ONE_ONE = 1  # e.g. "row1_val1"
    ONE_MANY = 2  # e.g. ("row1_val1", "row1_val_2")
    MANY_ONE = 3  # e.g. ["row1_val1", "row2_val_1", ...]
    MANY_MANY = 4  # e.g. [("row1_val1", "row1_val_2"), ("row2_val1", "row2_val_2"), ...]
