python_sources(
    skip_black=True,
)

pex_binary(
    name="sgr",
    entry_point="splitgraph.commandline:cli",
    # Add dynamically loaded hooks/plugins
    dependencies=[
        "src/py/splitgraph/splitgraph/ingestion/airbyte",
        "src/py/splitgraph/splitgraph/ingestion/dbt",
        "src/py/splitgraph/splitgraph/ingestion/singer",
        "src/py/splitgraph/splitgraph/ingestion/socrata",
        "src/py/splitgraph/splitgraph/ingestion/snowflake",
        "src/py/splitgraph/splitgraph/ingestion/athena",
        "src/py/splitgraph/splitgraph/ingestion/bigquery",
        "src/py/splitgraph/splitgraph/hooks",
    ],
)
