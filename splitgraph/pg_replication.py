import json

from splitgraph.constants import get_random_object_id

from splitgraph.meta_handler import get_all_tables, get_table, register_table_object

LOGICAL_DECODER = 'wal2json'


def generate_slot_name(mountpoint):
    return mountpoint


def start_replication(conn, mountpoint):
    with conn.cursor() as cur:
        cur.execute("""SELECT 'init' FROM pg_create_logical_replication_slot(%s, %s)""",
                    (generate_slot_name(mountpoint), LOGICAL_DECODER))


def stop_replication(conn, mountpoint):
    slot = generate_slot_name(mountpoint)
    with conn.cursor() as cur:
        cur.execute("""SELECT 1 FROM pg_replication_slots WHERE slot_name = %s""", (slot,))
        if cur.fetchone() is not None:
            cur.execute("""SELECT 'stop' FROM pg_drop_replication_slot(%s)""",
                        (slot,))


def consume_changes(conn, mountpoint):
    # Make sure the decoder only returns changes for the user tables in the
    # current mountpoint.
    # Consuming changes means they won't be returned again from this slot. Since we're skipping over
    # changes for other mountpoints, we have to have multiple logical slots.
    table_names = get_all_tables(conn, mountpoint)
    table_filter_param = ','.join('%s.%s' % (mountpoint, t.replace(' ', '\\ ')) for t in table_names),

    with conn.cursor() as cur:
        cur.execute("""SELECT data FROM pg_logical_slot_get_changes(%s, NULL, NULL, 'add-tables', %s)""",
                    (generate_slot_name(mountpoint), table_filter_param))

        return cur.fetchall()


KIND = {'insert': 0, 'delete': 1, 'update': 2}


def record_changes(conn, mountpoint, changes, HEAD, new_snap):
    # Decode the changeset produced by wal2json, remove redundant information from it
    # and save it to object tables.
    all_tables = get_all_tables(conn, mountpoint)
    with conn.cursor() as cur:
        table_objects = {}
        def get_table_object(t):
            if t not in table_objects:
                object_id = get_random_object_id()
                with conn.cursor() as cur:
                    # kind: 0 is added, 1 is removed, 2 is updated.
                    # change: json
                    fq_table = cur.mogrify('%s.%s' % (mountpoint, object_id))
                    cur.execute("""CREATE TABLE %s (kind smallint, change varchar)""" % fq_table)
                table_objects[t] = object_id
            return cur.mogrify('%s.%s' % (mountpoint, table_objects[t]))

        for changeset in changes:
            changeset = json.loads(changeset[0])['change']
            print (changeset)
            for change in changeset:
                # Delete things that we can infer in other ways, leave only columnnames/types/values.
                # import pdb; pdb.set_trace()
                kind = change.pop('kind')
                del change['schema']
                table = change.pop('table')

                if table not in all_tables:
                    continue

                fq_table = get_table_object(table)

                cur.execute("INSERT INTO %s VALUES (%%s, %%s)" % fq_table, (KIND[kind], json.dumps(change)))

    # Register the new commit.
    for t in all_tables:
        if t not in table_objects:
            table_info = get_table(conn, mountpoint, t, HEAD)
            if table_info is None:
                # If table created, store it as a snap
                with conn.cursor() as cur:
                    cur.execute("""CREATE TABLE %s AS SELECT * FROM %s""" %
                                (cur.mogrify('%s.%s' % (mountpoint, get_random_object_id())),
                                 cur.mogrify('%s.%s' % (mountpoint, t))))
                    register_table_object(conn, mountpoint, t, new_snap, get_random_object_id(), object_format='SNAP')
            else:
                prev_object_id, prev_format = table_info
                register_table_object(conn, mountpoint, t, new_snap, prev_object_id, prev_format)
        else:
            register_table_object(conn, mountpoint, t, new_snap, table_objects[t], object_format='WAL')


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
                query = """DELETE FROM %s WHERE""" % fq_staging_name
                query += ' AND '.join(
                    "%s.%s = %s" % (fq_staging_name, cur.mogrify(c), cur.mogrify(v))
                    for c, v in zip(change['oldkeys']['keynames'], change['oldkeys']['keyvalues']))
                queries.append(query)
            elif change_kind == 2:  # Update
                query = """UPDATE %s SET """
                query += ', '.join("%s.%s = %s" % (fq_staging_name, cur.mogrify(c), cur.mogrify(v))
                                   for c, v in zip(change['newkeys']['keynames'], change['newkeys']['keyvalues']))
                query += " WHERE "
                query += ' AND '.join(
                    "%s.%s = %s" % (fq_staging_name, cur.mogrify(c), cur.mogrify(v))
                    for c, v in zip(change['oldkeys']['keynames'], change['oldkeys']['keyvalues']))
                queries.append(query)

        cur.execute(b';'.join(queries))  # maybe some pagination needed here.
