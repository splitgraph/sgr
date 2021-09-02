"""
Functions for communicating with the remote Splitgraph catalog
"""

from typing import TYPE_CHECKING, Optional, Union, cast

from psycopg2.sql import SQL, Identifier
from splitgraph.config import SPLITGRAPH_API_SCHEMA, SPLITGRAPH_META_SCHEMA
from splitgraph.engine import ResultShape

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine


def get_info_key(engine: "PostgresEngine", key: str) -> Optional[str]:
    """
    Gets a configuration key from the remote registry, used to notify the client of the registry's capabilities.

    :param engine: Engine
    :param key: Key to get
    """
    return cast(
        Optional[str],
        engine.run_sql(
            SQL("SELECT value FROM {}.info WHERE key = %s").format(
                Identifier(SPLITGRAPH_META_SCHEMA)
            ),
            (key,),
            return_shape=ResultShape.ONE_ONE,
        ),
    )


def set_info_key(engine: "PostgresEngine", key: str, value: Union[bool, str]) -> None:
    """
    Sets a configuration value on the remote registry.

    :param engine: Engine
    :param key: Key to set
    :param value: New value for the key
    """

    engine.run_sql(
        SQL(
            "INSERT INTO {0}.info (key, value) VALUES (%s, %s)"
            " ON CONFLICT (key) DO UPDATE SET value = excluded.value WHERE info.key = excluded.key"
        ).format(Identifier(SPLITGRAPH_META_SCHEMA)),
        (key, value),
    )


def setup_registry_mode(engine: "PostgresEngine") -> None:
    """
    Set up access policies/RLS:

    * Normal users aren't allowed to create tables/schemata (can't do checkouts inside of a registry or
      upload SG objects directly to it)
    * Normal users can't access the splitgraph_meta schema directly: they're only supposed to be able to
      talk to it via stored procedures in splitgraph_api. Those procedures are set up with SECURITY INVOKER
      (run with those users' credentials) and what they can access is further restricted by RLS:

      * images/tables/tags meta tables: can only create/update/delete records where the namespace = user ID
      * objects/object_location tables: same. An object (piece of data) becomes owned by the user that creates
        it and still remains so even if someone else's image starts using it. Hence, the original owner can delete
        or change it (since they control the external location they've uploaded it to anyway).

    """

    if get_info_key(engine, "registry_mode") == "true":
        return

    for schema in (SPLITGRAPH_META_SCHEMA, SPLITGRAPH_API_SCHEMA):
        engine.run_sql(SQL("REVOKE CREATE ON SCHEMA {} FROM PUBLIC").format(Identifier(schema)))
        # Allow schema usage (including splitgraph_meta) but don't actually allow writing to/reading from
        # meta tables. This is so that users can still download objects that are stored directly
        # on the registry.
        engine.run_sql(SQL("GRANT USAGE ON SCHEMA {} TO PUBLIC").format(Identifier(schema)))

    # Allow everyone to read objects that have been uploaded
    engine.run_sql(
        SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO PUBLIC").format(
            Identifier(SPLITGRAPH_META_SCHEMA)
        )
    )

    set_info_key(engine, "registry_mode", "true")
