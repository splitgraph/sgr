import psycopg2

from splitgraph.constants import SPLITGRAPH_META_SCHEMA, _log
from splitgraph.meta_handler import get_table, get_snap_parent, ensure_metadata_schema, register_mountpoint, \
    unregister_mountpoint
from splitgraph.pg_replication import stop_replication, record_pending_changes, discard_pending_changes, \
    start_replication


def pg_table_exists(conn, mountpoint, table_name):
    # WTF: postgres quietly truncates all table names to 63 characters
    # at creation and in select statements
    with conn.cursor() as cur:
        cur.execute("""SELECT table_name from information_schema.tables
                       WHERE table_schema = %s AND table_name = %s""", (mountpoint, table_name[:63]))
        return cur.fetchone() is not None


def _get_column_names(conn, mountpoint, table_name):
    with conn.cursor() as cur:
        cur.execute("""SELECT column_name FROM information_schema.columns
                       WHERE table_schema = %s
                       AND table_name = %s""", (mountpoint, table_name))
        return [c[0] for c in cur.fetchall()]


def _table_exists_at(conn, mountpoint, table_name, image):
    return pg_table_exists(conn, mountpoint, table_name) if image is None \
        else bool(get_table(conn, mountpoint, table_name, image))


def make_conn(server, port, username, password, dbname):
    return psycopg2.connect(host=server, port=port, user=username, password=password, dbname=dbname)


def get_parent_children(conn, mountpoint, snap_id):
    parent = get_snap_parent(conn, mountpoint, snap_id)

    with conn.cursor() as cur:
        cur.execute("""SELECT snap_id FROM %s.snap_tree WHERE mountpoint = %%s AND parent_id = %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint, snap_id))
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
    ensure_metadata_schema(conn)
    # Initializes an empty repo with an initial commit (hash 0000...)
    with conn.cursor() as cur:
        cur.execute("""CREATE SCHEMA %s""" % cur.mogrify(mountpoint))
    snap_id = '0' * 64
    register_mountpoint(conn, mountpoint, snap_id, tables=[], table_object_ids=[])


def mount_postgres(conn, server, port, username, password, mountpoint, extra_options):
    with conn.cursor() as cur:
        dbname = extra_options['dbname']

        _log("postgres_fdw: importing foreign schema...")

        cur.execute("""CREATE SERVER %s
                        FOREIGN DATA WRAPPER postgres_fdw
                        OPTIONS (host %%s, port %%s, dbname %%s)""" %
                    cur.mogrify(mountpoint + '_server'), (server, str(port), dbname))
        cur.execute("""CREATE USER MAPPING FOR clientuser
                        SERVER %s
                        OPTIONS (user %%s, password %%s)""" % cur.mogrify(mountpoint + '_server'), (username, password))

        tables = extra_options.get('tables', [])
        remote_schema = extra_options['remote_schema']

        cur.execute("""CREATE SCHEMA %s""" % cur.mogrify(mountpoint))

        # Construct a query: import schema limit to (%s, %s, ...) from server mountpoint_server into mountpoint
        query = """IMPORT FOREIGN SCHEMA %s """ % cur.mogrify(remote_schema)
        if tables:
            query += "LIMIT TO (" + ",".join("%s" for _ in tables) + ") "
        query += "FROM SERVER %s INTO %s" % (cur.mogrify(mountpoint + '_server'), cur.mogrify(mountpoint))
        cur.execute(query, tables)


def mount_mongo(conn, server, port, username, password, mountpoint, extra_options):
    with conn.cursor() as cur:
        _log("mongo_fdw: mounting foreign tables...")

        fq_server = cur.mogrify(mountpoint + '_server')

        cur.execute("""CREATE SERVER %s
                        FOREIGN DATA WRAPPER mongo_fdw
                        OPTIONS (address %%s, port %%s)""" % fq_server, (server, str(port)))
        cur.execute("""CREATE USER MAPPING FOR clientuser
                        SERVER %s
                        OPTIONS (username %%s, password %%s)""" % fq_server, (username, password))

        cur.execute("""CREATE SCHEMA %s""" % cur.mogrify(mountpoint))

        # Mongo extra options: a map of
        # {table_name: {db: remote_db_name, coll: remote_collection_name, schema: {col1: type1, col2: type2...}}}
        for table_name, table_options in extra_options.items():
            _log("Mounting table %s" % table_name)
            db = table_options['db']
            coll = table_options['coll']

            query = "CREATE FOREIGN TABLE %s (_id NAME " % cur.mogrify('%s.%s' % (mountpoint, table_name))
            if table_options['schema']:
                query += ',' + ','.join(
                    "%s %s" % (cur.mogrify(cname), cur.mogrify(ctype)) for cname, ctype in
                    iter(table_options['schema'].items()))
            query += ") SERVER %s OPTIONS (database %%s, collection %%s)" % fq_server

            cur.execute(query, (db, coll))


def unmount(conn, mountpoint):
    ensure_metadata_schema(conn)
    record_pending_changes(conn)
    stop_replication(conn)
    discard_pending_changes(conn, mountpoint)
    # Make sure to consume and discard changes to this mountpoint if they exist, otherwise they might
    # be applied/recorded if a new mountpoint with the same name appears.

    with conn.cursor() as cur:
        cur.execute("""DROP SCHEMA IF EXISTS %s CASCADE""" % cur.mogrify(mountpoint))
        # Drop server too if it exists (could have been a non-foreign mountpoint)
        cur.execute("""DROP SERVER IF EXISTS %s CASCADE""" % cur.mogrify(mountpoint + '_server', ))

    # Currently we just discard all history info about the mounted schema
    unregister_mountpoint(conn, mountpoint)
    conn.commit()
    start_replication(conn)


def dump_table_creation(conn, schema, tables, created_schema=None):
    queries = []

    with conn.cursor() as cur:
        for t in tables:
            cur.execute("""SELECT column_name, data_type, is_nullable
                           FROM information_schema.columns
                           WHERE table_name = %s AND table_schema = %s""", (t, schema))
            cols = cur.fetchall()
            target = cur.mogrify("%s.%s" % (created_schema, t)) if created_schema else cur.mogrify(t)
            query = """CREATE TABLE %s (""" % target \
                    + ",".join("%s %s %s" % (cur.mogrify(cname), ctype, "NOT NULL" if not cnull else "")
                               for cname, ctype, cnull in cols) + ");"
            queries.append(query)
    return ';'.join(queries)