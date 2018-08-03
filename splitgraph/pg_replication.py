import json

from psycopg2.extras import execute_batch

from splitgraph.meta_handler import get_current_mountpoints_hashes, get_table_with_format

from splitgraph.constants import get_random_object_id, SPLITGRAPH_META_SCHEMA

from splitgraph.meta_handler import get_all_tables, get_table, register_table_object

LOGICAL_DECODER = 'wal2json'
REPLICATION_SLOT = 'sg_replication'


def _replication_slot_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""SELECT 1 FROM pg_replication_slots WHERE slot_name = %s""", (REPLICATION_SLOT,))
        return cur.fetchone() is not None


def start_replication(conn):
    with conn.cursor() as cur:
        cur.execute("""SELECT 'init' FROM pg_create_logical_replication_slot(%s, %s)""",
                    (REPLICATION_SLOT, LOGICAL_DECODER))


def stop_replication(conn):
    if _replication_slot_exists(conn):
        with conn.cursor() as cur:
            cur.execute("""SELECT 'stop' FROM pg_drop_replication_slot(%s)""", (REPLICATION_SLOT,))


def _consume_changes(conn):
    # Consuming changes means they won't be returned again from this slot. Perhaps worth doing peeking first
    # in case we crash after consumption but before recording changes.
    if not _replication_slot_exists(conn):
        return []

    all_mountpoints = [m[0] for m in get_current_mountpoints_hashes(conn)]
    table_filter_param = ','.join('%s.*' % m for m in all_mountpoints)

    with conn.cursor() as cur:
        cur.execute("""SELECT data FROM pg_logical_slot_get_changes(%s, NULL, NULL, 'add-tables', %s)""",
                    (REPLICATION_SLOT, table_filter_param))

        return cur.fetchall()


KIND = {'insert': 0, 'delete': 1, 'update': 2}


def record_pending_changes(conn):
    changes = _consume_changes(conn)
    if not changes:
        return
    # Decode the changeset produced by wal2json and save it to the pending_changes table.
    mountpoints_tables = {m: get_all_tables(conn, m) for m, _ in get_current_mountpoints_hashes(conn)}
    to_insert = []  # a list of tuples (schema, table, kind, change)
    for changeset in changes:
        changeset = json.loads(changeset[0])['change']
        for change in changeset:
            # kind: 0 is added, 1 is removed, 2 is updated.
            # change: json
            kind = change.pop('kind')
            mountpoint = change.pop('schema')
            table = change.pop('table')

            if mountpoint not in mountpoints_tables or table not in mountpoints_tables[mountpoint]:
                continue

            to_insert.append((mountpoint, table, KIND[kind], json.dumps(change)))

    with conn.cursor() as cur:
        execute_batch(cur, """INSERT INTO %s.%s VALUES (%%s, %%s, %%s, %%s)""" % (SPLITGRAPH_META_SCHEMA, "pending_changes"), to_insert)


def discard_pending_changes(conn, mountpoint):
    with conn.cursor() as cur:
        cur.execute("""DELETE FROM %s.%s WHERE mountpoint = %%s""" % (SPLITGRAPH_META_SCHEMA, "pending_changes"), (mountpoint,))


def has_pending_changes(conn, mountpoint):
    record_pending_changes(conn)
    with conn.cursor() as cur:
        cur.execute("""SELECT 1 FROM %s.%s WHERE mountpoint = %%s""" % (SPLITGRAPH_META_SCHEMA, "pending_changes"), (mountpoint,))
        return cur.fetchone() is not None


def dump_pending_changes(conn, mountpoint, table):
    # First, make sure we're up to date on changes.
    record_pending_changes(conn)
    with conn.cursor() as cur:
        cur.execute("""SELECT kind, change FROM %s.%s WHERE mountpoint = %%s AND table_name = %%s"""
                    % (SPLITGRAPH_META_SCHEMA, "pending_changes"), (mountpoint, table))
        return cur.fetchall()


def commit_pending_changes(conn, mountpoint, HEAD, new_image, include_snap=False):
    all_tables = get_all_tables(conn, mountpoint)
    with conn.cursor() as cur:
        cur.execute("""SELECT DISTINCT(table_name) FROM %s.%s
                       WHERE mountpoint = %%s""" % (SPLITGRAPH_META_SCHEMA, "pending_changes"), (mountpoint,))
        changed_tables = [c[0] for c in cur.fetchall()]
        for table in all_tables:
            table_info = get_table(conn, mountpoint, table, HEAD)
            # Table already exists at the current HEAD
            if table_info:
                with conn.cursor() as cur:
                    if table in changed_tables:
                        object_id = get_random_object_id()
                        fq_table = cur.mogrify('%s.%s' % (mountpoint, object_id))
                        # Move changes from the pending table to the actual object ID
                        cur.execute("""CREATE TABLE %s AS 
                                       WITH to_commit AS (DELETE FROM %s.%s WHERE mountpoint = %%s AND table_name = %%s
                                                          RETURNING kind, change)
                                       SELECT * FROM to_commit""" % (fq_table, SPLITGRAPH_META_SCHEMA, "pending_changes"),
                                    (mountpoint, table))
                        register_table_object(conn, mountpoint, table, new_image, object_id, object_format='DIFF')
                    else:
                        # If the table wasn't changed, point the commit to the old table objects (including
                        # any of snaps or diffs).
                        # This feels slightly weird: are we denormalized here?
                        for prev_object_id, prev_format in table_info:
                            register_table_object(conn, mountpoint, table, new_image, prev_object_id, prev_format)

            # If table created (or we want to store a snap anyway), copy the whole table over as well.
            if not table_info or include_snap:
                # Make sure we didn't actually create a snap for this table.
                if get_table_with_format(conn, mountpoint, table, HEAD, 'SNAP') is None:
                    with conn.cursor() as cur:
                        object_id = get_random_object_id()
                        cur.execute("""CREATE TABLE %s AS SELECT * FROM %s""" %
                                    (cur.mogrify('%s.%s' % (mountpoint, object_id)),
                                     cur.mogrify('%s.%s' % (mountpoint, table))))
                        register_table_object(conn, mountpoint, table, new_image, object_id, object_format='SNAP')


def apply_record_to_staging(conn, mountpoint, object_id, destination):
    with conn.cursor() as cur:
        fq_snap_name = cur.mogrify('%s.%s' % (mountpoint, object_id))
        fq_staging_name = cur.mogrify('%s.%s' % (mountpoint, destination))

        cur.execute("""SELECT * FROM %s""" % fq_snap_name)
        changes = cur.fetchall()
        queries = []
        for change_kind, change in changes:
            change = json.loads(change)
            if change_kind == 0:  # Insert
                column_names = change['columnnames']
                query = """INSERT INTO %s (%s) VALUES """ % (fq_staging_name, ','.join(cur.mogrify(c) for c in column_names))
                query += "(" + ','.join(['%s'] * (len(column_names))) + ")"
                query = cur.mogrify(query, change['columnvalues'])
                queries.append(query)
            elif change_kind == 1:  # Delete
                query = """DELETE FROM %s WHERE """ % fq_staging_name
                query += ' AND '.join(
                    "%s.%s = %s" % (fq_staging_name, cur.mogrify(c), cur.mogrify('%s', (v,)))
                    for c, v in zip(change['oldkeys']['keynames'], change['oldkeys']['keyvalues']))
                queries.append(query)
            elif change_kind == 2:  # Update
                query = """UPDATE %s SET """ % fq_staging_name
                query += ', '.join("%s = %s" % (cur.mogrify(c), cur.mogrify('%s', (v,)))
                                   for c, v in zip(change['oldkeys']['keynames'], change['columnvalues']))
                query += " WHERE "
                query += ' AND '.join(
                    "%s = %s" % (cur.mogrify(c), cur.mogrify('%s', (v,)))
                    for c, v in zip(change['oldkeys']['keynames'], change['oldkeys']['keyvalues']))
                queries.append(query)

        cur.execute(b';'.join(queries))  # maybe some pagination needed here.
