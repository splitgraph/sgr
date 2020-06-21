from dbt.adapters.postgres.impl import PostgresAdapter

from dbt.adapters.splitgraph import SplitgraphConnectionManager


class SplitgraphAdapter(PostgresAdapter):
    ConnectionManager = SplitgraphConnectionManager
