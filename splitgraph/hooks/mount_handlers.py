"""
Hooks for additional handlers used to mount other databases via FDW. These handlers become available
in the command line tool (via `sgr mount`) and in the Splitfile interpreter (via `FROM MOUNT`).
"""

import logging
from importlib import import_module

from psycopg2.sql import Identifier, SQL

from splitgraph.config import PG_USER, CONFIG
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


def _init_fdw(engine, server_id, wrapper, server_options, user_options):
    from splitgraph.engine.postgres.engine import PostgresEngine
    if not isinstance(engine, PostgresEngine):
        raise SplitGraphException("Only PostgresEngines support mounting via FDW!")

    engine.run_sql(SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(server_id)), return_shape=None)
    create_server = SQL("CREATE SERVER {} FOREIGN DATA WRAPPER {}").format(Identifier(server_id), Identifier(wrapper))

    server_keys, server_vals = zip(*server_options.items())
    if server_options:
        create_server += SQL(" OPTIONS (") \
                         + SQL(",").join(Identifier(o) + SQL(" %s") for o in server_keys) + SQL(")")
    engine.run_sql(create_server, server_vals, return_shape=None)

    user_keys, user_vals = zip(*user_options.items())
    create_mapping = SQL("CREATE USER MAPPING FOR {} SERVER {}").format(Identifier(PG_USER), Identifier(server_id))
    if user_options:
        create_mapping += SQL(" OPTIONS (") \
                          + SQL(",").join(Identifier(o) + SQL(" %s") for o in user_keys) + SQL(")")
    engine.run_sql(create_mapping, user_vals, return_shape=None)


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
    server_id = mountpoint + '_server'

    _init_fdw(engine, server_id, "postgres_fdw", {'host': server, 'port': str(port), 'dbname': dbname},
              {'user': username, 'password': password})

    engine.run_sql(SQL("CREATE SCHEMA {}").format(Identifier(mountpoint)), return_shape=None)

    # Construct a query: import schema limit to (%s, %s, ...) from server mountpoint_server into mountpoint
    query = "IMPORT FOREIGN SCHEMA {} "
    if tables:
        query += "LIMIT TO (" + ",".join("%s" for _ in tables) + ") "
    query += "FROM SERVER {} INTO {}"
    engine.run_sql(SQL(query).format(Identifier(remote_schema), Identifier(server_id),
                                     Identifier(mountpoint)), tables, return_shape=None)


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
    :param table_spec: A dictionary of form `{"table_name": {"db": <dbname>, "coll": <collection>, "schema": {"col1": "type1"...}}}`.
    """
    engine = get_engine()
    server_id = mountpoint + '_server'
    _init_fdw(engine, server_id, "mongo_fdw", {"address": server, "port": str(port)},
              {"username": username, "password": password})

    engine.run_sql(SQL("""CREATE SCHEMA {}""").format(Identifier(mountpoint)), return_shape=None)

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
        engine.run_sql(query, (db, coll), return_shape=None)


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
    :param dbname: Remote database name.
    :param remote_schema: Remote schema name.
    :param tables: Tables to mount (default all).
    """
    engine = get_engine()
    logging.info("Mounting foreign MySQL database...")
    server_id = mountpoint + '_server'

    _init_fdw(engine, server_id, "mysql_fdw", {"host": server, "port": str(port)},
              {"username": username, "password": password})

    engine.run_sql(SQL("CREATE SCHEMA {}").format(Identifier(mountpoint)), return_shape=None)

    query = "IMPORT FOREIGN SCHEMA {} "
    if tables:
        query += "LIMIT TO (" + ",".join("%s" for _ in tables) + ") "
    query += "FROM SERVER {} INTO {}"
    engine.run_sql(SQL(query).format(Identifier(remote_schema), Identifier(server_id),
                                     Identifier(mountpoint)), tables, return_shape=None)


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
