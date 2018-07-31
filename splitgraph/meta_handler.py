from psycopg2.extras import execute_batch

from splitgraph.constants import SPLITGRAPH_META_SCHEMA, SplitGraphException, _log


def _create_metadata_schema(conn):
    # Creates the metadata schema splitgraph_meta that stores
    # the hash tree of schema snaps and the current HEAD pointers.
    # This means we can't mount anything under the schema splitgraph_meta
    # -- much like we can't have a folder ".git" under Git version control...

    # This all should probably be moved into some sort of a setup function that runs when the
    # whole driver is set up for the first time.

    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA %s" % SPLITGRAPH_META_SCHEMA)
        # maybe FK parent_id on snap_id. NULL there means this is the repo root.
        cur.execute("""CREATE TABLE %s.%s (
                        snap_id    VARCHAR NOT NULL,
                        mountpoint VARCHAR NOT NULL,
                        parent_id  VARCHAR,
                        PRIMARY KEY (mountpoint, snap_id))""" % (SPLITGRAPH_META_SCHEMA, "snap_tree"))
        cur.execute("""CREATE TABLE %s.%s (
                        mountpoint VARCHAR NOT NULL,
                        snap_id    VARCHAR,
                        CONSTRAINT sh_fk FOREIGN KEY (mountpoint, snap_id) REFERENCES %s.%s)"""
                    % (SPLITGRAPH_META_SCHEMA, "snap_head", SPLITGRAPH_META_SCHEMA, "snap_tree"))

        # Maps a given table at a given point in time to an "object ID" (either a full snapshot or a
        # delta to a previous table).
        cur.execute("""CREATE TABLE %s.%s (
                        mountpoint VARCHAR NOT NULL,
                        snap_id    VARCHAR NOT NULL,
                        table_name VARCHAR NOT NULL,
                        object_id  VARCHAR NOT NULL,
                        format     VARCHAR NOT NULL,
                        PRIMARY KEY (mountpoint, snap_id, table_name),
                        CONSTRAINT tb_fk FOREIGN KEY (mountpoint, snap_id) REFERENCES %s.%s)"""
                    % (SPLITGRAPH_META_SCHEMA, "tables", SPLITGRAPH_META_SCHEMA, "snap_tree"))


def ensure_metadata_schema(conn):
    # Check if the metadata schema actually exists.
    with conn.cursor() as cur:
        cur.execute("""SELECT 1 FROM information_schema.schemata WHERE schema_name = %s""", (SPLITGRAPH_META_SCHEMA,))
        if cur.fetchone() is None:
            _create_metadata_schema(conn)


def get_all_tables(conn, mountpoint):
    # Gets all user tables in the current mountpoint, tracked or untracked
    with conn.cursor() as cur:
        cur.execute(
            """SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s and table_type = 'BASE TABLE'""", (mountpoint,))
        all_table_names = set([c[0] for c in cur.fetchall()])

        # Exclude all snapshots/packed tables
        cur.execute("""SELECT object_id FROM %s.tables WHERE mountpoint = %%s""" % SPLITGRAPH_META_SCHEMA, (mountpoint,))
        sys_tables = [c[0] for c in cur.fetchall()]
        return list(all_table_names.difference(sys_tables))


def get_tables_at(conn, mountpoint, snap_id):
    # Returns all table names mounted at mountpoint.
    with conn.cursor() as cur:
        cur.execute("""SELECT table_name
        FROM %s.tables WHERE mountpoint = %%s AND snap_id = %%s""" % SPLITGRAPH_META_SCHEMA, (mountpoint, snap_id))
        return [t[0] for t in cur.fetchall()]


def get_table(conn, mountpoint, table_name, snap_id):
    # Returns (object_id, object_format) from the table meta
    with conn.cursor() as cur:
        cur.execute("""SELECT object_id, format from %s.tables 
                       WHERE mountpoint = %%s AND snap_id = %%s AND table_name = %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint, snap_id, table_name))
        return cur.fetchone()


def register_table_object(conn, mountpoint, table, snap_id, object_id, object_format):
    with conn.cursor() as cur:
        query = """INSERT INTO %s.tables (mountpoint, snap_id, table_name, object_id, format)""" % SPLITGRAPH_META_SCHEMA
        query += """ VALUES (%s, %s, %s, %s, %s)"""
        cur.execute(query, (mountpoint, snap_id, table, object_id, object_format))


def get_all_foreign_tables(conn, mountpoint):
    # Inspects the information_schema to see which foreign tables we have in a given mountpoint.
    # Used in the beginning to populate the metadata since if we did IMPORT FOREIGN SCHEMA we've no idea what tables
    # we actually fetched from the remote postgres.
    with conn.cursor() as cur:
        cur.execute(
            """SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s and table_type = 'FOREIGN TABLE'""", (mountpoint,))
        return [c[0] for c in cur.fetchall()]


def get_current_head(conn, mountpoint, raise_on_none=True):
    ensure_metadata_schema(conn)
    with conn.cursor() as cur:
        cur.execute("""SELECT snap_id FROM %s.snap_head WHERE mountpoint = %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint,))
        result = cur.fetchone()
        if result is None or result == (None,):
            if not mountpoint_exists(conn, mountpoint):
                raise SplitGraphException("%s is not mounted." % mountpoint)
            elif raise_on_none:
                raise SplitGraphException("No current checked out revision found for %s. Check one out with \"sg "
                                          "checkout MOUNTPOINT SNAP_ID\"." % mountpoint)
            else:
                return None
        return result[0]


def add_new_snap_id(conn, mountpoint, parent_id, snap_id):
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO %s.snap_tree (snap_id, mountpoint, parent_id) VALUES (%%s, %%s, %%s)"""
                    % SPLITGRAPH_META_SCHEMA, (snap_id, mountpoint, parent_id))


def set_head(conn, mountpoint, snap_id):
    with conn.cursor() as cur:
        cur.execute("""SELECT 1 FROM %s.snap_head WHERE mountpoint = %%s""" % SPLITGRAPH_META_SCHEMA, (mountpoint,))
        if cur.fetchone() is None:
            cur.execute("""INSERT INTO %s.snap_head (snap_id, mountpoint) VALUES (%%s, %%s)""" % SPLITGRAPH_META_SCHEMA,
                        (snap_id,
                         mountpoint))
        else:
            cur.execute("""UPDATE %s.snap_head SET snap_id = %%s WHERE mountpoint = %%s""" % SPLITGRAPH_META_SCHEMA,
                        (snap_id,
                         mountpoint))


def mountpoint_exists(conn, mountpoint):
    with conn.cursor() as cur:
        # Check if the metadata schema actually exists.
        cur.execute("""SELECT 1 FROM information_schema.schemata WHERE schema_name = %s""", (mountpoint,))
        return cur.fetchone() is not None


def register_mountpoint(conn, mountpoint, snap_id, tables, table_object_ids):
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO %s.%s (snap_id, mountpoint, parent_id) VALUES (%%s, %%s, NULL)"""
                    % (SPLITGRAPH_META_SCHEMA, "snap_tree"), (snap_id, mountpoint))
        # Strictly speaking this is redundant since the checkout (of the "HEAD" commit) updates the head table.
        cur.execute("""INSERT INTO %s.%s (mountpoint, snap_id) VALUES (%%s, %%s)"""
                    % (SPLITGRAPH_META_SCHEMA, "snap_head"), (mountpoint, snap_id))
        for t, ti in zip(tables, table_object_ids):
            # Register the tables and the object IDs they were stored under
            cur.execute("""INSERT INTO %s.%s (mountpoint, snap_id, table_name, object_id, format)
                           VALUES (%%s, %%s, %%s, %%s, 'SNAP')"""
                        % (SPLITGRAPH_META_SCHEMA, "tables"), (mountpoint, snap_id, t, ti))


def unregister_mountpoint(conn, mountpoint):
    with conn.cursor() as cur:
        for meta_table in ["tables", "snap_head", "snap_tree"]:
            cur.execute("""DELETE FROM %s.%s WHERE mountpoint = %%s"""
                        % (SPLITGRAPH_META_SCHEMA, meta_table), (mountpoint,))


def get_snap_parent(conn, mountpoint, snap_id):
    with conn.cursor() as cur:
        cur.execute("""SELECT parent_id FROM %s.snap_tree WHERE mountpoint = %%s AND snap_id = %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint, snap_id))
        return cur.fetchone()[0]


def get_all_snap_parents(conn, mountpoint):
    with conn.cursor() as cur:
        cur.execute("""SELECT snap_id, parent_id FROM %s.snap_tree WHERE mountpoint = %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint,))
        return cur.fetchall()


def get_canonical_snap_id(conn, mountpoint, short_snap):
    with conn.cursor() as cur:
        cur.execute("""SELECT snap_id FROM %s.snap_tree WHERE mountpoint = %%s AND snap_id LIKE %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint, short_snap.lower() + '%'))
        candidates = [c[0] for c in cur.fetchall()]

    if len(candidates) == 0:
        raise SplitGraphException("No snapshots beginning with %s found for mountpoint %s!" % (short_snap, mountpoint))

    if len(candidates) > 1:
        result = "Multiple suitable candidates found: \n"
        for c in candidates:
            result += " * %s\n" % c
        raise SplitGraphException(result)

    return candidates[0]


def register_objects(conn, mountpoint, object_meta):
    object_meta = [(mountpoint, ) + o for o in object_meta]
    with conn.cursor() as cur:
        query = """INSERT INTO %s.tables (mountpoint, snap_id, table_name, object_id, format)""" % SPLITGRAPH_META_SCHEMA
        query += """ VALUES (%s, %s, %s, %s, %s)"""
        execute_batch(cur, query, object_meta, page_size=100)


def get_existing_objects(conn, mountpoint):
    with conn.cursor() as cur:
        cur.execute("""SELECT object_id from %s.tables WHERE mountpoint = %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint,))
        return [c[0] for c in cur.fetchall()]