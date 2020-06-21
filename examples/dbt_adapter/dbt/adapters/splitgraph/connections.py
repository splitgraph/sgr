import re
from dataclasses import dataclass
from typing import Tuple

import agate
from dbt.adapters.postgres import PostgresConnectionManager, PostgresCredentials
from psycopg2.pool import AbstractConnectionPool

from splitgraph.core.repository import Repository
from splitgraph.engine import switch_engine
from splitgraph.engine.postgres.engine import PostgresEngine
from splitgraph.splitfile.execution import ImageMapper


@dataclass
class SplitgraphCredentials(PostgresCredentials):
    @property
    def type(self):
        return "splitgraph"


_SCHEMA_RE = re.compile(r'"([\w-]+/)?([\w-]+):([\w-]+)"')


def _rewrite_splitgraph_images(sql, image_mapper):
    # This code is partially adapted from the Splitfile executor. A more robust
    # way of doing this is preparsing the SQL statements with pglast but then this won't be
    # runnable on Windows. The tradeoff is that comments in dbt code that match this regex
    # will also be treated as Splitgraph images.
    for namespace, repository, image_hash in _SCHEMA_RE.findall(sql):
        original_text = namespace + repository + ":" + image_hash
        if namespace:
            # Strip the / at the end
            namespace = namespace[:-1]

        temporary_schema, canonical_name = image_mapper(
            Repository(namespace, repository), image_hash
        )
        sql = sql.replace(original_text, temporary_schema)

    return sql


class FakeConnectionPool(AbstractConnectionPool):
    """Connection pool that returns a single connection: dbt handles
    pooling and thread safety for us, so we reuse the Postgres connection that
    the PostgresConnectionManager has."""

    def __init__(self, connection, *args, **kwargs):
        super().__init__(minconn=0, maxconn=0, *args, **kwargs)
        self.connection = connection

    def getconn(self, key=None):
        return self.connection

    def putconn(self, conn, key=None, close=False):
        assert conn == self.connection

    def closeall(self):
        pass


class SplitgraphConnectionManager(PostgresConnectionManager):
    TYPE = "splitgraph"

    def __init__(self, *args, **kwargs):
        self._engine_initialized = False
        super().__init__(*args, **kwargs)

    def get_splitgraph_engine(self):
        # We assume the engine is initialized already. Initializing it here requires an
        # out-of-band admin connection that might mess with dbt's connection handling.
        return PostgresEngine(
            name="LOCAL", pool=FakeConnectionPool(self.get_thread_connection().handle)
        )

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[str, agate.Table]:
        engine = self.get_splitgraph_engine()
        image_mapper = ImageMapper(engine)
        try:
            with switch_engine(engine):
                sql = _rewrite_splitgraph_images(sql, image_mapper)
                image_mapper.setup_lq_mounts()
            return super().execute(sql, auto_begin, fetch)
        finally:
            image_mapper.teardown_lq_mounts()
