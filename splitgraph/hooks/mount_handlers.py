"""
Hooks for additional handlers used to mount other databases via FDW. These handlers become available
in the command line tool (via `sgr mount`) and in the Splitfile interpreter (via `FROM MOUNT`).
"""

import logging
from importlib import import_module

from psycopg2.sql import Identifier, SQL
from splitgraph.config import PG_USER, CONFIG
from splitgraph.core._common import ensure_metadata_schema
from splitgraph.engine import get_engine
from splitgraph.exceptions import SplitGraphException

_MOUNT_HANDLERS = {}


def get_mount_handler(mount_handler):
    """Returns a mount function for a given handler.
    The mount function must have a signature `(mountpoint, server, port, username, password, handler_kwargs)`."""
    try:
        return _MOUNT_HANDLERS[mount_handler]
    except KeyError:
        raise SplitGraphException("Mount handler %s not supported!" % mount_handler)


def get_mount_handlers():
    """Returns the names of all registered mount handlers."""
    return list(_MOUNT_HANDLERS.keys())


def register_mount_handler(name, mount_function):
    """Returns a mount function under a given name. See `get_mount_handler` for the mount handler spec."""
    global _MOUNT_HANDLERS
    if name in _MOUNT_HANDLERS:
        raise SplitGraphException("Cannot register a mount handler %s as it already exists!" % name)
    _MOUNT_HANDLERS[name] = mount_function


def init_fdw(engine, server_id, wrapper, server_options=None, user_options=None, overwrite=True):
    """
    Sets up a foreign data server on the engine.

    :param engine: PostgresEngine
    :param server_id: Name to call the foreign server, must be unique. Will be deleted if exists.
    :param wrapper: Name of the foreign data wrapper (must be installed as an extension on the engine)
    :param server_options: Dictionary of FDW options
    :param user_options: Dictionary of user options
    :param overwrite: If the server already exists, delete and recreate it.
    """
    from splitgraph.engine.postgres.engine import PostgresEngine
    if not isinstance(engine, PostgresEngine):
        raise SplitGraphException("Only PostgresEngines support mounting via FDW!")

    if overwrite:
        engine.run_sql(SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(server_id)))

    create_server = SQL("CREATE SERVER IF NOT EXISTS {} FOREIGN DATA WRAPPER {}") \
        .format(Identifier(server_id), Identifier(wrapper))

    if server_options:
        server_keys, server_vals = zip(*server_options.items())
        create_server += SQL(" OPTIONS (") \
                         + SQL(",").join(Identifier(o) + SQL(" %s") for o in server_keys) + SQL(")")
        engine.run_sql(create_server, server_vals)
    else:
        engine.run_sql(create_server)

    if user_options:
        create_mapping = SQL("CREATE USER MAPPING IF NOT EXISTS FOR {} SERVER {}") \
            .format(Identifier(PG_USER), Identifier(server_id))
        user_keys, user_vals = zip(*user_options.items())
        create_mapping += SQL(" OPTIONS (") \
                          + SQL(",").join(Identifier(o) + SQL(" %s") for o in user_keys) + SQL(")")
        engine.run_sql(create_mapping, user_vals)


def mount_postgres(mountpoint, server, port, username, password, dbname, remote_schema, tables=[]):
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
    engine = get_engine()
    logging.info("Importing foreign Postgres schema...")

    # Name foreign servers based on their targets so that we can reuse them.
    server_id = '%s_%s_%s_server' % (server, str(port), dbname)
    init_fdw(engine, server_id, "postgres_fdw", {'host': server, 'port': str(port), 'dbname': dbname},
             {'user': username, 'password': password}, overwrite=False)

    engine.run_sql(SQL("CREATE SCHEMA {}").format(Identifier(mountpoint)))

    # Construct a query: import schema limit to (%s, %s, ...) from server mountpoint_server into mountpoint
    query = "IMPORT FOREIGN SCHEMA {} "
    if tables:
        query += "LIMIT TO (" + ",".join("%s" for _ in tables) + ") "
    query += "FROM SERVER {} INTO {}"
    engine.run_sql(SQL(query).format(Identifier(remote_schema), Identifier(server_id),
                                     Identifier(mountpoint)), tables)


def mount_mongo(mountpoint, server, port, username, password, **table_spec):
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
    engine = get_engine()
    server_id = mountpoint + '_server'
    init_fdw(engine, server_id, "mongo_fdw", {"address": server, "port": str(port)},
             {"username": username, "password": password})

    engine.run_sql(SQL("""CREATE SCHEMA {}""").format(Identifier(mountpoint)))

    # Parse the table spec
    # {table_name: {db: remote_db_name, coll: remote_collection_name, schema: {col1: type1, col2: type2...}}}
    for table_name, table_options in table_spec.items():
        logging.info("Mounting table %s", table_name)
        db = table_options['db']
        coll = table_options['coll']

        query = SQL("CREATE FOREIGN TABLE {}.{} (_id NAME ").format(Identifier(mountpoint), Identifier(table_name))
        if table_options['schema']:
            for cname, ctype in table_options['schema'].items():
                query += SQL(", {} %s" % ctype).format(Identifier(cname))
        query += SQL(") SERVER {} OPTIONS (database %s, collection %s)").format(Identifier(server_id))
        engine.run_sql(query, (db, coll))


def mount_mysql(mountpoint, server, port, username, password, remote_schema, tables=[]):
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
    engine = get_engine()
    logging.info("Mounting foreign MySQL database...")
    server_id = mountpoint + '_server'

    init_fdw(engine, server_id, "mysql_fdw", {"host": server, "port": str(port)},
             {"username": username, "password": password})

    engine.run_sql(SQL("CREATE SCHEMA {}").format(Identifier(mountpoint)))

    query = "IMPORT FOREIGN SCHEMA {} "
    if tables:
        query += "LIMIT TO (" + ",".join("%s" for _ in tables) + ") "
    query += "FROM SERVER {} INTO {}"
    engine.run_sql(SQL(query).format(Identifier(remote_schema), Identifier(server_id),
                                     Identifier(mountpoint)), tables)


# Register the mount handlers from the config.
for handler_name, handler_func_name in CONFIG.get('mount_handlers', {}).items():
    ix = handler_func_name.rindex('.')
    try:
        handler_func = getattr(import_module(handler_func_name[:ix]), handler_func_name[ix + 1:])
        register_mount_handler(handler_name.lower(), handler_func)
    except AttributeError as e:
        raise SplitGraphException("Error loading custom mount handler {0}".format(handler_name), e)
    except ImportError as e:
        raise SplitGraphException("Error loading custom mount handler {0}".format(handler_name), e)


def mount(mountpoint, mount_handler, handler_kwargs):
    """
    Mounts a foreign database via Postgres FDW (without creating new Splitgraph objects)

    :param mountpoint: Mountpoint to import the new tables into.
    :param mount_handler: The type of the mounted database. Must be one of `postgres_fdw` or `mongo_fdw`.
    :param handler_kwargs: Dictionary of options to pass to the mount handler.
    """
    engine = get_engine()
    ensure_metadata_schema(engine)
    mh_func = get_mount_handler(mount_handler)
    logging.info("Connecting to remote server...")

    engine.run_sql(SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(mountpoint)))
    engine.run_sql(SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(mountpoint + '_server')))
    mh_func(mountpoint, **handler_kwargs)
    engine.commit()
