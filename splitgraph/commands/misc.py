import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.sql import SQL, Identifier

from splitgraph.constants import SPLITGRAPH_META_SCHEMA, _log
from splitgraph.meta_handler import get_table, get_snap_parent, ensure_metadata_schema, register_mountpoint, \
    unregister_mountpoint, get_object_meta, META_TABLES
from splitgraph.pg_replication import record_pending_changes, discard_pending_changes, stop_replication, \
    start_replication
from splitgraph.pg_utils import pg_table_exists


def _table_exists_at(conn, mountpoint, table_name, image):
    return pg_table_exists(conn, mountpoint, table_name) if image is None \
        else bool(get_table(conn, mountpoint, table_name, image))


def make_conn(server, port, username, password, dbname):
    return psycopg2.connect(host=server, port=port, user=username, password=password, dbname=dbname)


def get_parent_children(conn, mountpoint, snap_id):
    parent = get_snap_parent(conn, mountpoint, snap_id)

    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT snap_id FROM {}.snap_tree WHERE mountpoint = %s AND parent_id = %s""").format(
            Identifier(SPLITGRAPH_META_SCHEMA)),
                    (mountpoint, snap_id))
        children = [c[0] for c in cur.fetchall()]
    return parent, children


def get_log(conn, mountpoint, start_snap):
    # Repeatedly gets the parent of a given snapshot until it reaches the bottom.
    result = []
    while start_snap is not None:
        result.append(start_snap)
        start_snap = get_snap_parent(conn, mountpoint, start_snap)
    return result


def _find_path(conn, mountpoint, snap_1, snap_2):
    path = []
    while snap_2 is not None:
        path.append(snap_2)
        snap_2 = get_snap_parent(conn, mountpoint, snap_2)
        if snap_2 == snap_1:
            return path


def init(conn, mountpoint):
    record_pending_changes(conn)
    stop_replication(conn)
    ensure_metadata_schema(conn)
    # Initializes an empty repo with an initial commit (hash 0000...)
    with conn.cursor() as cur:
        cur.execute(SQL("CREATE SCHEMA {}").format(Identifier(mountpoint)))
    snap_id = '0' * 64
    register_mountpoint(conn, mountpoint, snap_id, tables=[], table_object_ids=[])
    conn.commit()
    start_replication(conn)


def mount_postgres(conn, server, port, username, password, mountpoint, extra_options):
    with conn.cursor() as cur:
        dbname = extra_options['dbname']

        _log("postgres_fdw: importing foreign schema...")

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
        _log("mongo_fdw: mounting foreign tables...")

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
            _log("Mounting table %s" % table_name)
            db = table_options['db']
            coll = table_options['coll']

            query = SQL("CREATE FOREIGN TABLE {}.{} (_id NAME ").format(Identifier(mountpoint), Identifier(table_name))
            if table_options['schema']:
                for cname, ctype in table_options['schema'].items():
                    query += SQL(", {} %s" % ctype).format(Identifier(cname))
            query += SQL(") SERVER {} OPTIONS (database %s, collection %s)").format(server_id)
            cur.execute(query, (db, coll))


def unmount(conn, mountpoint):
    ensure_metadata_schema(conn)
    # Make sure to consume and discard changes to this mountpoint if they exist, otherwise they might
    # be applied/recorded if a new mountpoint with the same name appears.
    record_pending_changes(conn)
    discard_pending_changes(conn, mountpoint)

    with conn.cursor() as cur:
        cur.execute(SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(mountpoint)))
        # Drop server too if it exists (could have been a non-foreign mountpoint)
        cur.execute(SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(mountpoint + '_server')))

    # Currently we just discard all history info about the mounted schema
    unregister_mountpoint(conn, mountpoint)


def cleanup_objects(conn):
    # Deletes all objects not required by any mountpoint.
    # There are probably several tiers of this (delete physical objects, delete their URLs from the meta
    # tables etc, but this does everything.

    # First, get a list of all objects required by a table.
    with conn.cursor() as cur:
        cur.execute(SQL("SELECT DISTINCT (object_id) FROM {}.tables").format(Identifier(SPLITGRAPH_META_SCHEMA)))
        primary_objects = set([c[0] for c in cur.fetchall()])

    # Expand that since each object might have a parent it depends on.
    if primary_objects:
        while True:
            new_parents = set(parent_id for _, _, parent_id in get_object_meta(conn, list(primary_objects))
                              if parent_id not in primary_objects)
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
