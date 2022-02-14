"""
Defines the interface for a Splitgraph engine (a backing database), including running basic SQL commands,
tracking tables for changes and uploading/downloading tables to other remote engines.

By default, Splitgraph is backed by Postgres: see :mod:`splitgraph.engine.postgres` for an example of how to
implement a different engine.
"""
import importlib.resources
import logging
from enum import Enum
from typing import TYPE_CHECKING

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core.migration import set_installed_version, source_files_to_apply
from splitgraph.resources import splitgraph_meta

if TYPE_CHECKING:
    from splitgraph.engine.postgres.psycopg import PsycopgEngine


class ResultShape(Enum):
    """Shape that the result of a query will be coerced to"""

    NONE = 0  # No result expected
    ONE_ONE = 1  # e.g. "row1_val1"
    ONE_MANY = 2  # e.g. ("row1_val1", "row1_val_2")
    MANY_ONE = 3  # e.g. ["row1_val1", "row2_val_1", ...]
    MANY_MANY = 4  # e.g. [("row1_val1", "row1_val_2"), ("row2_val1", "row2_val_2"), ...]


META_TABLES = [
    "images",
    "tags",
    "objects",
    "tables",
    "upstream",
    "object_locations",
    "object_cache_status",
    "object_cache_occupancy",
    "info",
    "version",
]


def ensure_metadata_schema(engine: "PsycopgEngine") -> None:
    """
    Create or migrate the metadata schema splitgraph_meta that stores the hash tree of schema
    snapshots (images), tags and tables.
    This means we can't mount anything under the schema splitgraph_meta -- much like we can't have a folder
    ".git" under Git version control...
    """

    schema_files = [f for f in importlib.resources.contents(splitgraph_meta) if f.endswith("sql")]

    files, target_version = source_files_to_apply(
        engine,
        schema_name=SPLITGRAPH_META_SCHEMA,
        schema_files=schema_files,
    )

    if not files:
        return
    for name in files:
        data = importlib.resources.read_text(splitgraph_meta, name)
        logging.info("Running %s", name)
        engine.run_sql(data)
    set_installed_version(engine, SPLITGRAPH_META_SCHEMA, target_version)
    engine.commit()
