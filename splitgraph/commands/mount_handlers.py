from psycopg2.sql import Identifier, SQL
from splitgraph.constants import SplitGraphException, log

MOUNT_HANDLERS = {}


def get_mount_handler(mount_handler):
    """Returns a mount function for a given handler.
    The mount function has a signature (conn, hostname, port, username, password, handler_options)."""
    try:
        return MOUNT_HANDLERS[mount_handler]
    except KeyError:
        raise SplitGraphException("Mount handler %s not supported!" % mount_handler)


def register_mount_handler(name, mount_function):
    """Returns a mount function under a given name.
        The mount function has a signature (conn, hostname, port, username, password, handler_options)."""
    global MOUNT_HANDLERS
    if name in MOUNT_HANDLERS:
        raise SplitGraphException("Cannot register a mount handler %s as it already exists!" % name)
    MOUNT_HANDLERS[name] = mount_function


def mount_postgres(conn, server, port, username, password, mountpoint, extra_options):
    """
    Mounts a schema on a remote Postgres database as a set of foreign tables locally.
    :param conn: Psycopg connection object.
    :param server: Database hostname.
    :param port: Port the Postgres server is running on.
    :param username: A read-only user that the database will be accessed as.
    :param password: Password for the read-only user.
    :param mountpoint: Schema to mount the remote into.
    :param extra_options: A dictionary of form {"dbname": <dbname>, "remote_schema": <remote schema>,
                                                "tables": <tables to mount (optional)>}
    """
    with conn.cursor() as cur:
        dbname = extra_options['dbname']

        log("postgres_fdw: importing foreign schema...")

        server_id = Identifier(mountpoint + '_server')

        cur.execute(SQL("""CREATE SERVER {}
                        FOREIGN DATA WRAPPER postgres_fdw
                        OPTIONS (host %s, port %s, dbname %s)""").format(server_id), (server, str(port), dbname))
        cur.execute(SQL("""CREATE USER MAPPING FOR clientuser
                        SERVER {}
                        OPTIONS (user %s, password %s)""").format(server_id), (username, password))

        tables = extra_options.get('tables', [])
        remote_schema = extra_options['remote_schema']

        cur.execute(SQL("CREATE SCHEMA {}").format(Identifier(mountpoint)))

        # Construct a query: import schema limit to (%s, %s, ...) from server mountpoint_server into mountpoint
        query = "IMPORT FOREIGN SCHEMA {} "
        if tables:
            query += "LIMIT TO (" + ",".join("%s" for _ in tables) + ") "
        query += "FROM SERVER {} INTO {}"
        cur.execute(SQL(query).format(Identifier(remote_schema), server_id, Identifier(mountpoint)), tables)


def mount_mongo(conn, server, port, username, password, mountpoint, extra_options):
    """
    Mounts one or more collections on a remote Mongo database as a set of foreign tables locally.
    :param conn: Psycopg connection object.
    :param server: Database hostname.
    :param port: Port the Mongo server is running on.
    :param username: A read-only user that the database will be accessed as.
    :param password: Password for the read-only user.
    :param mountpoint: Schema to mount the remote into.
    :param extra_options: A dictionary of form {"table_name": {"db": <dbname>, "coll": <collection>,
                                                               "schema": {"col1": "type1"...}}}.
    """
    with conn.cursor() as cur:
        log("mongo_fdw: mounting foreign tables...")

        server_id = Identifier(mountpoint + '_server')

        cur.execute(SQL("""CREATE SERVER {}
                        FOREIGN DATA WRAPPER mongo_fdw
                        OPTIONS (address %s, port %s)""").format(server_id), (server, str(port)))
        cur.execute(SQL("""CREATE USER MAPPING FOR clientuser
                        SERVER {}
                        OPTIONS (username %s, password %s)""").format(server_id), (username, password))

        cur.execute(SQL("""CREATE SCHEMA {}""").format(Identifier(mountpoint)))

        # Mongo extra options: a map of
        # {table_name: {db: remote_db_name, coll: remote_collection_name, schema: {col1: type1, col2: type2...}}}
        for table_name, table_options in extra_options.items():
            log("Mounting table %s" % table_name)
            db = table_options['db']
            coll = table_options['coll']

            query = SQL("CREATE FOREIGN TABLE {}.{} (_id NAME ").format(Identifier(mountpoint), Identifier(table_name))
            if table_options['schema']:
                for cname, ctype in table_options['schema'].items():
                    query += SQL(", {} %s" % ctype).format(Identifier(cname))
            query += SQL(") SERVER {} OPTIONS (database %s, collection %s)").format(server_id)
            cur.execute(query, (db, coll))


# Register the default mount handlers. Maybe in the future we can put this into the config instead.
register_mount_handler("postgres_fdw", mount_postgres)
register_mount_handler("mongo_fdw", mount_mongo)
