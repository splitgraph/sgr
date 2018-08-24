import psycopg2
from psycopg2.sql import SQL, Identifier

from splitgraph.constants import SPLITGRAPH_META_SCHEMA, log
from splitgraph.meta_handler import get_table, get_snap_parent, register_mountpoint, \
    unregister_mountpoint, get_object_meta, META_TABLES, ensure_metadata_schema
from splitgraph.pg_replication import discard_pending_changes, suspend_replication, record_pending_changes
from splitgraph.pg_utils import pg_table_exists


def table_exists_at(conn, mountpoint, table_name, image_hash):
    return pg_table_exists(conn, mountpoint, table_name) if image_hash is None \
        else bool(get_table(conn, mountpoint, table_name, image_hash))


def make_conn(server, port, username, password, dbname):
    return psycopg2.connect(host=server, port=port, user=username, password=password, dbname=dbname)


def get_parent_children(conn, mountpoint, image_hash):
    """Gets the parent and a list of children of a given image."""
    parent = get_snap_parent(conn, mountpoint, image_hash)

    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT snap_id FROM {}.snap_tree WHERE mountpoint = %s AND parent_id = %s""").format(
            Identifier(SPLITGRAPH_META_SCHEMA)),
            (mountpoint, image_hash))
        children = [c[0] for c in cur.fetchall()]
    return parent, children


def get_log(conn, mountpoint, start_snap):
    """Repeatedly gets the parent of a given snapshot until it reaches the bottom."""
    result = []
    while start_snap is not None:
        result.append(start_snap)
        start_snap = get_snap_parent(conn, mountpoint, start_snap)
    return result


def find_path(conn, mountpoint, hash_1, hash_2):
    """If the two images are on the same path in the commit tree, returns that path."""
    path = []
    while hash_2 is not None:
        path.append(hash_2)
        hash_2 = get_snap_parent(conn, mountpoint, hash_2)
        if hash_2 == hash_1:
            return path


@suspend_replication
def init(conn, mountpoint):
    """
    Initializes an empty repo with an initial commit (hash 0000...)
    :param conn: psycopg connection object.
    :param mountpoint: Mountpoint to create the repository in. Must not exist.
    """
    with conn.cursor() as cur:
        cur.execute(SQL("CREATE SCHEMA {}").format(Identifier(mountpoint)))
    snap_id = '0' * 64
    register_mountpoint(conn, mountpoint, snap_id, tables=[], table_object_ids=[])
    conn.commit()


def mount_postgres(conn, server, port, username, password, mountpoint, extra_options):
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


def unmount(conn, mountpoint):
    """
    Discards all changes to a given mountpoint and all of its history, deleting the physical Postgres schema.
    Doesn't delete any cached physical objects.
    :param conn: psycopg connection object
    :param mountpoint: Mountpoint to unmount.
    :return:
    """
    # Make sure to discard changes to this mountpoint if they exist, otherwise they might
    # be applied/recorded if a new mountpoint with the same name appears.
    ensure_metadata_schema(conn)
    record_pending_changes(conn)
    discard_pending_changes(conn, mountpoint)

    with conn.cursor() as cur:
        cur.execute(SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(mountpoint)))
        # Drop server too if it exists (could have been a non-foreign mountpoint)
        cur.execute(SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(mountpoint + '_server')))

    # Currently we just discard all history info about the mounted schema
    unregister_mountpoint(conn, mountpoint)
    conn.commit()


def cleanup_objects(conn):
    """
    Deletes all local objects not required by any current mountpoint, including their dependencies, their remote
    locations and their cached local copies.
    :param conn: psycopg connection object.
    """
    # First, get a list of all objects required by a table.
    with conn.cursor() as cur:
        cur.execute(SQL("SELECT DISTINCT (object_id) FROM {}.tables").format(Identifier(SPLITGRAPH_META_SCHEMA)))
        primary_objects = set([c[0] for c in cur.fetchall()])

    # Expand that since each object might have a parent it depends on.
    if primary_objects:
        while True:
            new_parents = set(parent_id for _, _, parent_id in get_object_meta(conn, list(primary_objects))
                              if parent_id not in primary_objects and parent_id is not None)
            if not new_parents:
                break
            else:
                primary_objects.update(new_parents)

    # Go through the tables that aren't mountpoint-dependent and delete entries there.
    with conn.cursor() as cur:
        for table_name in ['object_tree', 'object_locations']:
            query = SQL("DELETE FROM {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(table_name))
            if primary_objects:
                query += SQL(" WHERE object_id NOT IN (" + ','.join('%s' for _ in range(len(primary_objects))) + ")")
            cur.execute(query, list(primary_objects))

    # Go through the physical objects and delete them as well
    with conn.cursor() as cur:
        cur.execute("""SELECT information_schema.tables.table_name FROM information_schema.tables
                        WHERE information_schema.tables.table_schema = %s""", (SPLITGRAPH_META_SCHEMA,))
        # This is slightly dirty, but since the info about the objects was deleted on unmount, we just say that
        # anything in splitgraph_meta that's not a system table is fair game.
        tables_in_meta = set(c[0] for c in cur.fetchall() if c[0] not in META_TABLES)

        to_delete = tables_in_meta.difference(primary_objects)
        if to_delete:
            cur.execute(SQL(";").join(SQL("DROP TABLE {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                     Identifier(d)) for d in to_delete))

        log("Deleted %d physical object(s)" % len(to_delete))
