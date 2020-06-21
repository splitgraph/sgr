from dbt.adapters.base import AdapterPlugin

from dbt.adapters.splitgraph.connections import SplitgraphConnectionManager
from dbt.adapters.splitgraph.connections import SplitgraphCredentials
from dbt.adapters.splitgraph.impl import SplitgraphAdapter

from dbt.include import splitgraph

Plugin = AdapterPlugin(
    adapter=SplitgraphAdapter,
    credentials=SplitgraphCredentials,
    include_path=splitgraph.PACKAGE_PATH,
    dependencies=["postgres"],
)
