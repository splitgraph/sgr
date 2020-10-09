import logging
from typing import Dict, Optional, List

from splitgraph.core.types import TableColumn
from splitgraph.hooks.data_source import init_fdw, create_foreign_table

# Define the schema of the foreign table we wish to create
# We're only going to be fetching stories, so limit the columns to the ones that
# show up for stories. See https://github.com/HackerNews/API for reference.
_story_schema_spec = [
    TableColumn(1, "id", "integer", True),
    TableColumn(2, "by", "text", False),
    TableColumn(3, "time", "integer", False),
    TableColumn(4, "title", "text", False),
    TableColumn(5, "url", "text", False),
    TableColumn(6, "text", "text", False),
    TableColumn(7, "score", "integer", False),
    TableColumn(8, "kids", "integer[]", False),
    TableColumn(9, "descendants", "integer", False),
]


def mount_hn(
    mountpoint: str, server, port, username, password, endpoints: Optional[List[str]] = None
) -> None:
    """
    Mount a Hacker News story dataset using the Firebase API.
    \b
    :param endpoints: List of Firebase endpoints to mount, mounted into the same tables as
    the endpoint name. Supported endpoints: {top,new,best,ask,show,job}stories.
    """

    # A mount handler is a function that takes five arguments and any number
    # of optional ones. The mountpoint is always necessary and shows which foreign schema
    # to mount the dataset into. The connection parameters can be None.

    from splitgraph.engine import get_engine
    from psycopg2.sql import Identifier, SQL

    engine = get_engine()
    server_id = mountpoint + "_server"

    # Define server options that are common for all tables managed by this wrapper
    options: Dict[str, Optional[str]] = {
        # Module path to our foreign data wrapper class on the engine side
        "wrapper": "hn_fdw.fdw.HNForeignDataWrapper",
    }

    # Initialize the FDW: this will create the foreign server and user mappings behind the scenes.
    init_fdw(engine, server_id=server_id, wrapper="multicorn", server_options=options)

    # Create the schema that we'll be putting foreign tables into.
    engine.run_sql(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(mountpoint)))

    endpoints = endpoints or [
        "topstories",
        "newstories",
        "beststories",
        "askstories",
        "showstories",
        "jobstories",
    ]

    for endpoint in endpoints:
        # Generate SQL required to create a foreign table in the target schema.
        # create_foreign_table handles the boilerplate around running CREATE FOREIGN TABLE
        # on the engine and passing necessary arguments.
        logging.info("Mounting %s...", endpoint)
        sql, args = create_foreign_table(
            schema=mountpoint,
            server=server_id,
            table_name=endpoint,
            schema_spec=_story_schema_spec,
            extra_options=None,
        )

        engine.run_sql(sql, args)

    # This was all run in a single SQL transaction so that everything gets rolled back
    # and we don't have to clean up in case of an error. We can commit it now.
    engine.commit()
