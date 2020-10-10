"""
Hooks for additional handlers used to mount other databases via FDW. These handlers become available
in the command line tool (via `sgr mount`) and in the Splitfile interpreter (via `FROM MOUNT`).
"""

from importlib import import_module
from typing import Dict, List, TYPE_CHECKING, Any, Type

from splitgraph.config import CONFIG
from splitgraph.config.config import get_all_in_section
from splitgraph.exceptions import MountHandlerError
from splitgraph.hooks.data_source import (
    ForeignDataWrapperDataSource,
)

if TYPE_CHECKING:
    pass

_MOUNT_HANDLERS: Dict[str, Type[ForeignDataWrapperDataSource]] = {}


def get_mount_handler(mount_handler: str) -> Type[ForeignDataWrapperDataSource]:
    """Returns a mount function for a given handler.
    The mount function must have a signature `(mountpoint, server, port, username, password, handler_kwargs)`."""
    try:
        return _MOUNT_HANDLERS[mount_handler]
    except KeyError:
        raise MountHandlerError("Mount handler %s not supported!" % mount_handler)


def get_mount_handlers() -> List[str]:
    """Returns the names of all registered mount handlers."""
    return list(_MOUNT_HANDLERS.keys())


def register_mount_handler(name: str, mount_class: Type[ForeignDataWrapperDataSource]) -> None:
    """Returns a data source under a given name. See `get_mount_handler` for the mount handler spec."""
    global _MOUNT_HANDLERS
    _MOUNT_HANDLERS[name] = mount_class


def mount_postgres(mountpoint, **kwargs) -> None:
    """
    Mount a Postgres database.

    Mounts a schema on a remote Postgres database as a set of foreign tables locally.
    \b

    :param mountpoint: Schema to mount the remote into.
    :param server: Database hostname.
    :param port: Port the Postgres server is running on.
    :param username: A read-only user that the database will be accessed as.
    :param password: Password for the read-only user.
    :param dbname: Remote database name.
    :param remote_schema: Remote schema name.
    :param extra_server_args: Dictionary of extra arguments to pass to the foreign server
    :param tables: Tables to mount (default all). If a list, then will use IMPORT FOREIGN SCHEMA.
    If a dictionary, must have the format {"table_name": {"col_1": "type_1", ...}}.
    """
    mount(mountpoint, mount_handler="postgres_fdw", handler_kwargs=kwargs)


def mount_mongo(
    mountpoint: str, server: str, port: int, username: str, password: str, **table_spec
) -> None:
    """
    Mount a Mongo database.

    Mounts one or more collections on a remote Mongo database as a set of foreign tables locally.
    \b

    :param mountpoint: Schema to mount the remote into.
    :param server: Database hostname.
    :param port: Port the Mongo server is running on.
    :param username: A read-only user that the database will be accessed as.
    :param password: Password for the read-only user.
    :param table_spec: A dictionary of form `{"table_name": {"db": <dbname>, "coll": <collection>,
        "schema": {"col1": "type1"...}}}`.
    """
    pass


def mount_elasticsearch(
    mountpoint: str,
    server: str,
    port: int,
    username: str,
    password: str,
    table_spec: Dict[str, Dict[str, Any]],
):
    """
    Mount an ElasticSearch instance.

    Mount a set of tables proxying to a remote ElasticSearch index.

    This uses a fork of postgres-elasticsearch-fdw behind the scenes. You can add a column
    `query` to your table and set it as `query_column` to pass advanced ES queries and aggregations.
    For example:

    ```
    sgr mount elasticsearch -c elasticsearch:9200 -o@- <<EOF
        {
          "table_spec": {
            "table_1": {
              "schema": {
                "id": "text",
                "@timestamp": "timestamp",
                "query": "text",
                "col_1": "text",
                "col_2": "boolean",
              },
              "index": "index-pattern*",
              "rowid_column": "id",
              "query_column": "query",
            }
          }
        }
    EOF
    ```
    \b
    """
    pass


def mount(
    mountpoint: str,
    mount_handler: str,
    handler_kwargs: Dict[str, Any],
) -> None:
    """
    Mounts a foreign database via an FDW (without creating new Splitgraph objects)

    :param mountpoint: Mountpoint to import the new tables into.
    :param mount_handler: The type of the mounted database. Must be one of `postgres_fdw` or `mongo_fdw`.
    :param handler_kwargs: Dictionary of options to pass to the mount handler.
    """
    from splitgraph.engine import get_engine
    from psycopg2.sql import Identifier, SQL

    engine = get_engine()
    data_source = get_mount_handler(mount_handler)

    engine.run_sql(SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(mountpoint)))
    engine.run_sql(
        SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(mountpoint + "_server"))
    )

    source = data_source.from_commandline(engine, handler_kwargs)
    source.mount(schema=mountpoint)
    engine.commit()


def _register_default_handlers() -> None:
    # Register the mount handlers from the config.
    for handler_name, handler_class_name in get_all_in_section(CONFIG, "mount_handlers").items():
        assert isinstance(handler_class_name, str)

        ix = handler_class_name.rindex(".")
        try:
            handler_class = getattr(
                import_module(handler_class_name[:ix]), handler_class_name[ix + 1 :]
            )
            assert issubclass(handler_class, ForeignDataWrapperDataSource)
            register_mount_handler(handler_name.lower(), handler_class)
        except AttributeError as e:
            raise MountHandlerError(
                "Error loading custom mount handler {0}".format(handler_name)
            ) from e
        except ImportError as e:
            raise MountHandlerError(
                "Error loading custom mount handler {0}".format(handler_name)
            ) from e


_register_default_handlers()
