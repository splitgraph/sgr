from datetime import datetime

from psycopg2.extras import execute_batch

from splitgraph.constants import SPLITGRAPH_META_SCHEMA, SplitGraphException, _log


def _create_metadata_schema(conn):
    # Creates the metadata schema splitgraph_meta that stores
    # the hash tree of schema snaps and the current tags.
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
                        created    TIMESTAMP,
                        comment    VARCHAR,
                        PRIMARY KEY (mountpoint, snap_id))""" % (SPLITGRAPH_META_SCHEMA, "snap_tree"))
        cur.execute("""CREATE TABLE %s.%s (
                        mountpoint VARCHAR NOT NULL,
                        snap_id    VARCHAR,
                        tag        VARCHAR,
                        PRIMARY KEY (mountpoint, tag),
                        CONSTRAINT sh_fk FOREIGN KEY (mountpoint, snap_id) REFERENCES %s.%s)"""
                    % (SPLITGRAPH_META_SCHEMA, "snap_tags", SPLITGRAPH_META_SCHEMA, "snap_tree"))

        # Maps a given table at a given point in time to an "object ID" (either a full snapshot or a
        # delta to a previous table).
        cur.execute("""CREATE TABLE %s.%s (
                        mountpoint VARCHAR NOT NULL,
                        snap_id    VARCHAR NOT NULL,
                        table_name VARCHAR NOT NULL,
                        object_id  VARCHAR NOT NULL,
                        format     VARCHAR NOT NULL,
                        PRIMARY KEY (mountpoint, snap_id, table_name, format),
                        CONSTRAINT tb_fk FOREIGN KEY (mountpoint, snap_id) REFERENCES %s.%s)"""
                    % (SPLITGRAPH_META_SCHEMA, "tables", SPLITGRAPH_META_SCHEMA, "snap_tree"))

        # Keep track of what the remotes for a given mountpoint are (by default, we create an "origin" remote
        # on initial pull)
        cur.execute("""CREATE TABLE %s.%s (
                        mountpoint         VARCHAR NOT NULL,
                        remote_name        VARCHAR NOT NULL,
                        remote_conn_string VARCHAR NOT NULL,
                        remote_mountpoint  VARCHAR NOT NULL,
                        PRIMARY KEY (mountpoint, remote_name))"""
                    % (SPLITGRAPH_META_SCHEMA, "remotes"))

        # Map objects to their locations for when they don't live on the remote or the local machine but instead
        # in S3/some FTP/HTTP server/torrent etc.
        # Lookup path to resolve an object on checkout: local -> this table -> remote (so that we don't bombard
        # the remote with queries for tables that may have been uploaded to a different place).
        cur.execute("""CREATE TABLE %s.%s (
                        mountpoint         VARCHAR NOT NULL,
                        object_id          VARCHAR NOT NULL,
                        location           VARCHAR NOT NULL,
                        protocol           VARCHAR NOT NULL,
                        PRIMARY KEY (mountpoint, object_id))"""
                    % (SPLITGRAPH_META_SCHEMA, "object_locations"))


def _create_pending_changes(conn):
    # We consume changes from the WAL for the tables that interest us into this schema so that
    # postgres can flush parts of the WAL that it doesn't needs.
    # Used on checkout/commit to keep track of changes that happened to a schema
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE %s.%s (
                        mountpoint VARCHAR NOT NULL,
                        table_name VARCHAR NOT NULL,
                        kind       SMALLINT,
                        change     VARCHAR NOT NULL)""" % (SPLITGRAPH_META_SCHEMA, "pending_changes"))


def ensure_metadata_schema(conn):
    # Check if the metadata schema actually exists.
    with conn.cursor() as cur:
        cur.execute("""SELECT 1 FROM information_schema.schemata WHERE schema_name = %s""", (SPLITGRAPH_META_SCHEMA,))
        if cur.fetchone() is None:
            _create_metadata_schema(conn)
            _create_pending_changes(conn)


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


def get_table_with_format(conn, mountpoint, table_name, snap_id, object_format):
    # Returns the object ID of a table at a given time, with a given format.
    with conn.cursor() as cur:
        cur.execute("""SELECT object_id from %s.tables 
                       WHERE mountpoint = %%s AND snap_id = %%s AND table_name = %%s AND format = %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint, snap_id, table_name, object_format))
        result = cur.fetchone()
        return None if result is None else result[0]


def get_table(conn, mountpoint, table_name, snap_id):
    # Returns a list of available[(object_id, object_format)] from the table meta
    with conn.cursor() as cur:
        cur.execute("""SELECT object_id, format from %s.tables 
                       WHERE mountpoint = %%s AND snap_id = %%s AND table_name = %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint, snap_id, table_name))
        return cur.fetchall()


def register_table_object(conn, mountpoint, table, snap_id, object_id, object_format):
    with conn.cursor() as cur:
        query = """INSERT INTO %s.tables (mountpoint, snap_id, table_name, object_id, format)""" % SPLITGRAPH_META_SCHEMA
        query += """ VALUES (%s, %s, %s, %s, %s)"""
        cur.execute(query, (mountpoint, snap_id, table, object_id, object_format))


def deregister_table_object(conn, mountpoint, object_id):
    with conn.cursor() as cur:
        query = """DELETE FROM %s.tables WHERE """ % SPLITGRAPH_META_SCHEMA
        query += """ object_id = %s"""
        cur.execute(query, (object_id,))


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
    return get_tagged_id(conn, mountpoint, 'HEAD', raise_on_none)


def get_tagged_id(conn, mountpoint, tag, raise_on_none=True):
    ensure_metadata_schema(conn)
    with conn.cursor() as cur:
        cur.execute("""SELECT snap_id FROM %s.snap_tags WHERE mountpoint = %%s AND tag = %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint, tag))
        result = cur.fetchone()
        if result is None or result == (None,):
            if not mountpoint_exists(conn, mountpoint):
                raise SplitGraphException("%s is not mounted." % mountpoint)
            elif raise_on_none:
                if tag == 'HEAD':
                    raise SplitGraphException("No current checked out revision found for %s. Check one out with \"sg "
                                              "checkout MOUNTPOINT SNAP_ID\"." % mountpoint)
                else:
                    raise SplitGraphException("Tag %s not found in mountpoint %s" % (tag, mountpoint))
            else:
                return None
        return result[0]


def get_all_hashes_tags(conn, mountpoint):
    with conn.cursor() as cur:
        cur.execute("""SELECT snap_id, tag from %s.snap_tags WHERE mountpoint = %%s""" % SPLITGRAPH_META_SCHEMA, (mountpoint,))
        return cur.fetchall()


def add_new_snap_id(conn, mountpoint, parent_id, snap_id, created=None, comment=None):
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO %s.snap_tree (snap_id, mountpoint, parent_id, created, comment)
                       VALUES (%%s, %%s, %%s, %%s, %%s)"""
                    % SPLITGRAPH_META_SCHEMA, (snap_id, mountpoint, parent_id, created or datetime.now(), comment))


def set_head(conn, mountpoint, snap_id):
    set_tag(conn, mountpoint, snap_id, 'HEAD', force=True)


def set_tag(conn, mountpoint, snap_id, tag, force=False):
    with conn.cursor() as cur:
        cur.execute("""SELECT 1 FROM %s.snap_tags WHERE mountpoint = %%s AND tag = %%s""" % SPLITGRAPH_META_SCHEMA, (mountpoint, tag))
        if cur.fetchone() is None:
            cur.execute("""INSERT INTO %s.snap_tags (snap_id, mountpoint, tag) VALUES (%%s, %%s, %%s)""" % SPLITGRAPH_META_SCHEMA,
                        (snap_id, mountpoint, tag))
        else:
            if force:
                cur.execute("""UPDATE %s.snap_tags SET snap_id = %%s WHERE mountpoint = %%s AND tag = %%s""" % SPLITGRAPH_META_SCHEMA,
                            (snap_id, mountpoint, tag))
            else:
                raise SplitGraphException("Tag %s already exists in mountpoint %s!" % (tag, mountpoint))


def mountpoint_exists(conn, mountpoint):
    with conn.cursor() as cur:
        # Check if the metadata schema actually exists.
        cur.execute("""SELECT 1 FROM information_schema.schemata WHERE schema_name = %s""", (mountpoint,))
        return cur.fetchone() is not None


def register_mountpoint(conn, mountpoint, snap_id, tables, table_object_ids):
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO %s.%s (snap_id, mountpoint, parent_id, created) VALUES (%%s, %%s, NULL, %%s)"""
                    % (SPLITGRAPH_META_SCHEMA, "snap_tree"), (snap_id, mountpoint, datetime.now()))
        # Strictly speaking this is redundant since the checkout (of the "HEAD" commit) updates the tag table.
        cur.execute("""INSERT INTO %s.%s (mountpoint, snap_id, tag) VALUES (%%s, %%s, 'HEAD')"""
                    % (SPLITGRAPH_META_SCHEMA, "snap_tags"), (mountpoint, snap_id))
        for t, ti in zip(tables, table_object_ids):
            # Register the tables and the object IDs they were stored under.
            # They're obviously stored as snaps since there's nothing to diff to...
            cur.execute("""INSERT INTO %s.%s (mountpoint, snap_id, table_name, object_id, format)
                           VALUES (%%s, %%s, %%s, %%s, 'SNAP')"""
                        % (SPLITGRAPH_META_SCHEMA, "tables"), (mountpoint, snap_id, t, ti))


def unregister_mountpoint(conn, mountpoint):
    with conn.cursor() as cur:
        for meta_table in ["tables", "snap_tags", "snap_tree", "remotes", "object_locations"]:
            cur.execute("""DELETE FROM %s.%s WHERE mountpoint = %%s"""
                        % (SPLITGRAPH_META_SCHEMA, meta_table), (mountpoint,))


def get_snap_parent(conn, mountpoint, snap_id):
    with conn.cursor() as cur:
        cur.execute("""SELECT parent_id FROM %s.snap_tree WHERE mountpoint = %%s AND snap_id = %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint, snap_id))
        return cur.fetchone()[0]


def get_all_snap_info(conn, mountpoint, snap_id):
    with conn.cursor() as cur:
        cur.execute("""SELECT parent_id, created, comment FROM %s.snap_tree WHERE mountpoint = %%s AND snap_id = %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint, snap_id))
        return cur.fetchone()


def get_all_snap_parents(conn, mountpoint):
    with conn.cursor() as cur:
        cur.execute("""SELECT snap_id, parent_id, created, comment FROM %s.snap_tree WHERE mountpoint = %%s
                       ORDER BY created"""
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


def register_object_locations(conn, mountpoint, object_locations):
    with conn.cursor() as cur:
        # Don't insert redundant objects here either.
        cur.execute("""SELECT object_id FROM %s.object_locations
                       WHERE mountpoint = %%s""" % SPLITGRAPH_META_SCHEMA, (mountpoint,))
        existing_locations = [c[0] for c in cur.fetchall()]
        object_locations = [(mountpoint,) + o for o in object_locations if o[0] not in existing_locations]

        query = """INSERT INTO %s.object_locations (mountpoint, object_id, location, protocol)""" % SPLITGRAPH_META_SCHEMA
        query += """ VALUES (%s, %s, %s, %s)"""
        execute_batch(cur, query, object_locations, page_size=100)


def get_existing_objects(conn, mountpoint):
    with conn.cursor() as cur:
        cur.execute("""SELECT object_id from %s.tables WHERE mountpoint = %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint,))
        return set(c[0] for c in cur.fetchall())


def get_downloaded_objects(conn, mountpoint):
    # Minor normalization sadness here: this can return duplicate object IDs since
    # we might repeat them if different versions of the same table point to the same object ID.
    with conn.cursor() as cur:
        cur.execute("""SELECT information_schema.tables.table_name FROM information_schema.tables JOIN %s.tables 
                        ON information_schema.tables.table_name = %s.tables.object_id
                        AND information_schema.tables.table_schema = mountpoint
                        WHERE mountpoint = %%s"""
                    % (SPLITGRAPH_META_SCHEMA, SPLITGRAPH_META_SCHEMA), (mountpoint,))
        return set(c[0] for c in cur.fetchall())


def get_current_mountpoints_hashes(conn):
    ensure_metadata_schema(conn)
    with conn.cursor() as cur:
        cur.execute("""SELECT mountpoint, snap_id FROM %s.snap_tags WHERE tag = 'HEAD'""" % SPLITGRAPH_META_SCHEMA)
        return cur.fetchall()


def get_remote_for(conn, mountpoint, remote_name='origin'):
    with conn.cursor() as cur:
        cur.execute("""SELECT remote_conn_string, remote_mountpoint FROM %s.remotes
                        WHERE mountpoint = %%s AND remote_name = %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint, remote_name))
        return cur.fetchone()


def add_remote(conn, mountpoint, remote_conn, remote_mountpoint, remote_name='origin'):
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO %s.remotes (mountpoint, remote_name, remote_conn_string, remote_mountpoint)
                        VALUES (%%s, %%s, %%s, %%s)"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint, remote_name, remote_conn, remote_mountpoint))


def get_external_object_locations(conn, mountpoint, objects):
    with conn.cursor() as cur:
        query = """SELECT object_id, location, protocol from %s.object_locations 
                       WHERE mountpoint = %%s AND object_id IN (""" % SPLITGRAPH_META_SCHEMA
        query += ','.join('%s' for _ in objects) + ")"
        cur.execute(query, [mountpoint] + objects)
        object_locations = cur.fetchall()
    return object_locations