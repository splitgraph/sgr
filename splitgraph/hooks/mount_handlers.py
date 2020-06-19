"""
Hooks for additional handlers used to mount other databases via FDW. These handlers become available
in the command line tool (via `sgr mount`) and in the Splitfile interpreter (via `FROM MOUNT`).
"""

import logging
from importlib import import_module
from typing import Callable, Dict, List, Optional, Union, TYPE_CHECKING, cast

from splitgraph.config import PG_USER, CONFIG
from splitgraph.config.config import get_all_in_section
from splitgraph.exceptions import MountHandlerError

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine

_MOUNT_HANDLERS: Dict[str, Callable] = {}


def get_mount_handler(mount_handler: str) -> Callable:
    """Returns a mount function for a given handler.
    The mount function must have a signature `(mountpoint, server, port, username, password, handler_kwargs)`."""
    try:
        return _MOUNT_HANDLERS[mount_handler]
    except KeyError:
        raise MountHandlerError("Mount handler %s not supported!" % mount_handler)


def get_mount_handlers() -> List[str]:
    """Returns the names of all registered mount handlers."""
    return list(_MOUNT_HANDLERS.keys())


def register_mount_handler(name: str, mount_function: Callable) -> None:
    """Returns a mount function under a given name. See `get_mount_handler` for the mount handler spec."""
    global _MOUNT_HANDLERS
    _MOUNT_HANDLERS[name] = mount_function


def init_fdw(
    engine: "PostgresEngine",
    server_id: str,
    wrapper: str,
    server_options: Optional[Dict[str, Union[str, None]]] = None,
    user_options: Optional[Dict[str, str]] = None,
    overwrite: bool = True,
) -> None:
    """
    Sets up a foreign data server on the engine.

    :param engine: PostgresEngine
    :param server_id: Name to call the foreign server, must be unique. Will be deleted if exists.
    :param wrapper: Name of the foreign data wrapper (must be installed as an extension on the engine)
    :param server_options: Dictionary of FDW options
    :param user_options: Dictionary of user options
    :param overwrite: If the server already exists, delete and recreate it.
    """
    from psycopg2.sql import Identifier, SQL

    if overwrite:
        engine.run_sql(SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(server_id)))

    create_server = SQL("CREATE SERVER IF NOT EXISTS {} FOREIGN DATA WRAPPER {}").format(
        Identifier(server_id), Identifier(wrapper)
    )

    if server_options:
        server_keys, server_vals = zip(*server_options.items())
        create_server += (
            SQL(" OPTIONS (")
            + SQL(",").join(Identifier(o) + SQL(" %s") for o in server_keys)
            + SQL(")")
        )
        engine.run_sql(create_server, server_vals)
    else:
        engine.run_sql(create_server)

    if user_options:
        create_mapping = SQL("CREATE USER MAPPING IF NOT EXISTS FOR {} SERVER {}").format(
            Identifier(PG_USER), Identifier(server_id)
        )
        user_keys, user_vals = zip(*user_options.items())
        create_mapping += (
            SQL(" OPTIONS (")
            + SQL(",").join(Identifier(o) + SQL(" %s") for o in user_keys)
            + SQL(")")
        )
        engine.run_sql(create_mapping, user_vals)


def mount_postgres(
    mountpoint: str,
    server: str,
    port: Union[int, str],
    username: str,
    password: str,
    dbname: str,
    remote_schema: str,
    tables: Optional[List[str]] = None,
) -> None:
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
    :param tables: Tables to mount (default all).
    """
    from splitgraph.engine import get_engine
    from psycopg2.sql import Identifier, SQL

    if tables is None:
        tables = []
    engine = get_engine()
    logging.info("Importing foreign Postgres schema...")

    # Name foreign servers based on their targets so that we can reuse them.
    server_id = "%s_%s_%s_server" % (server, str(port), dbname)
    init_fdw(
        engine,
        server_id,
        "postgres_fdw",
        {"host": server, "port": str(port), "dbname": dbname},
        {"user": username, "password": password},
        overwrite=False,
    )

    # Allow mounting tables into existing schemas
    engine.run_sql(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(mountpoint)))
    _import_foreign_schema(engine, mountpoint, remote_schema, server_id, tables)


def _import_foreign_schema(
    engine: "PostgresEngine", mountpoint: str, remote_schema: str, server_id: str, tables: List[str]
) -> None:
    from psycopg2.sql import Identifier, SQL

    # Construct a query: import schema limit to (%s, %s, ...) from server mountpoint_server into mountpoint
    query = SQL("IMPORT FOREIGN SCHEMA {} ").format(Identifier(remote_schema))
    if tables:
        query += SQL("LIMIT TO (") + SQL(",").join(Identifier(t) for t in tables) + SQL(")")
    query += SQL("FROM SERVER {} INTO {}").format(Identifier(server_id), Identifier(mountpoint))
    engine.run_sql(query)


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
    from splitgraph.engine import get_engine
    from psycopg2.sql import Identifier, SQL

    engine = get_engine()
    server_id = mountpoint + "_server"
    init_fdw(
        engine,
        server_id,
        "mongo_fdw",
        {"address": server, "port": str(port)},
        {"username": username, "password": password},
    )

    engine.run_sql(SQL("""CREATE SCHEMA IF NOT EXISTS {}""").format(Identifier(mountpoint)))

    # Parse the table spec
    # {table_name: {db: remote_db_name, coll: remote_collection_name, schema: {col1: type1, col2: type2...}}}
    for table_name, table_options in table_spec.items():
        logging.info("Mounting table %s", table_name)
        db = table_options["db"]
        coll = table_options["coll"]

        query = SQL("CREATE FOREIGN TABLE {}.{} (_id NAME ").format(
            Identifier(mountpoint), Identifier(table_name)
        )
        if table_options["schema"]:
            for cname, ctype in table_options["schema"].items():
                query += SQL(", {} %s" % ctype).format(Identifier(cname))
        query += SQL(") SERVER {} OPTIONS (database %s, collection %s)").format(
            Identifier(server_id)
        )
        engine.run_sql(query, (db, coll))


def mount_mysql(
    mountpoint: str,
    server: str,
    port: int,
    username: str,
    password: str,
    remote_schema: str,
    tables: List[str] = None,
) -> None:
    """
    Mount a MySQL database.

    Mounts a schema on a remote MySQL database as a set of foreign tables locally.
    \b

    :param mountpoint: Schema to mount the remote into.
    :param server: Database hostname.
    :param port: Database port
    :param username: A read-only user that the database will be accessed as.
    :param password: Password for the read-only user.
    :param remote_schema: Remote schema name.
    :param tables: Tables to mount (default all).
    """
    from splitgraph.engine import get_engine
    from psycopg2.sql import Identifier, SQL

    if tables is None:
        tables = []
    engine = get_engine()
    logging.info("Mounting foreign MySQL database...")
    server_id = mountpoint + "_server"

    init_fdw(
        engine,
        server_id,
        "mysql_fdw",
        {"host": server, "port": str(port)},
        {"username": username, "password": password},
    )

    engine.run_sql(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(mountpoint)))
    _import_foreign_schema(engine, mountpoint, remote_schema, server_id, tables)


def mount(
    mountpoint: str,
    mount_handler: str,
    handler_kwargs: Dict[
        str, Union[str, int, None, List[str], Dict[str, Union[str, Dict[str, str]]]]
    ],
) -> None:
    """
    Mounts a foreign database via Postgres FDW (without creating new Splitgraph objects)

    :param mountpoint: Mountpoint to import the new tables into.
    :param mount_handler: The type of the mounted database. Must be one of `postgres_fdw` or `mongo_fdw`.
    :param handler_kwargs: Dictionary of options to pass to the mount handler.
    """
    from splitgraph.engine import get_engine
    from psycopg2.sql import Identifier, SQL

    engine = get_engine()
    mh_func = get_mount_handler(mount_handler)

    engine.run_sql(SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(mountpoint)))
    engine.run_sql(
        SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(mountpoint + "_server"))
    )
    mh_func(mountpoint, **handler_kwargs)
    engine.commit()


def _register_default_handlers() -> None:
    # Register the mount handlers from the config.
    for handler_name, handler_func_name in get_all_in_section(CONFIG, "mount_handlers").items():
        assert isinstance(handler_func_name, str)

        ix = handler_func_name.rindex(".")
        try:
            handler_func = getattr(
                import_module(handler_func_name[:ix]), handler_func_name[ix + 1 :]
            )
            register_mount_handler(handler_name.lower(), handler_func)
        except AttributeError as e:
            raise MountHandlerError(
                "Error loading custom mount handler {0}".format(handler_name)
            ) from e
        except ImportError as e:
            raise MountHandlerError(
                "Error loading custom mount handler {0}".format(handler_name)
            ) from e


_register_default_handlers()
