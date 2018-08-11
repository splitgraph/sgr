import json

from psycopg2.extras import execute_batch
from psycopg2.sql import SQL, Identifier

from splitgraph.meta_handler import get_current_mountpoints_hashes, get_table_with_format

from splitgraph.constants import get_random_object_id, SPLITGRAPH_META_SCHEMA, SplitGraphException

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
        execute_batch(cur, SQL("INSERT INTO {}.{} VALUES (%s, %s, %s, %s)").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                                   Identifier("pending_changes")),
                      to_insert)


def discard_pending_changes(conn, mountpoint):
    with conn.cursor() as cur:
        cur.execute(SQL("DELETE FROM {}.{} WHERE mountpoint = %s").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                          Identifier("pending_changes")),
                    (mountpoint,))


def has_pending_changes(conn, mountpoint):
    record_pending_changes(conn)
    with conn.cursor() as cur:
        cur.execute(SQL("SELECT 1 FROM {}.{} WHERE mountpoint = %s").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                            Identifier("pending_changes")),
                    (mountpoint,))
        return cur.fetchone() is not None


def dump_pending_changes(conn, mountpoint, table, aggregate=False):
    # First, make sure we're up to date on changes.
    record_pending_changes(conn)
    with conn.cursor() as cur:
        if aggregate:
            cur.execute(SQL(
                "SELECT kind, count(kind) FROM {}.{} WHERE mountpoint = %s AND table_name = %s GROUP BY kind").format(
                Identifier(SPLITGRAPH_META_SCHEMA),
                Identifier("pending_changes")),
                (mountpoint, table))
            return cur.fetchall()
        else:
            cur.execute(SQL("SELECT kind, change FROM {}.{} WHERE mountpoint = %s AND table_name = %s").format(
                Identifier(SPLITGRAPH_META_SCHEMA),
                Identifier("pending_changes")),
                (mountpoint, table))
            repl_id = _get_replica_identity(conn, mountpoint, table)
            ri_cols, _ = zip(*repl_id)
            result = []
            for kind, change in cur:
                result.append(_convert_wal_change(kind, json.loads(change), ri_cols))
            return result


def _get_primary_keys(conn, mountpoint, table):
    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT c.column_name, c.data_type
                           FROM information_schema.table_constraints tc
                            JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
                            JOIN information_schema.columns AS c 
                            ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
                           WHERE constraint_type = 'PRIMARY KEY' AND tc.table_schema = %s AND tc.table_name = %s"""), (mountpoint, table))
        return cur.fetchall()


def _get_replica_identity(conn, mountpoint, table):
    return _get_primary_keys(conn, mountpoint, table) or _get_column_names_types(conn, mountpoint, table)


def _create_diff_table(conn, mountpoint, object_id, replica_identity_cols_types):
    # Create a diff table into which we'll pack the conflated WAL actions:
    # replica identity -- multiple columns forming the table's PK (or all rows), PKd.
    # sg_action_kind: 0, 1, 2 for insert/delete/update
    # sg_action_data: extra data for insert and update.
    with conn.cursor() as cur:
        query = SQL("CREATE TABLE {}.{} (").format(Identifier(mountpoint), Identifier(object_id))
        query += SQL(',').join(SQL("{} %s" % col_type).format(Identifier(col_name))
                               for col_name, col_type in replica_identity_cols_types + [('sg_action_kind', 'smallint'),
                                                                                        ('sg_action_data', 'varchar')])
        query += SQL(", PRIMARY KEY (") + SQL(',').join(
            SQL("{}").format(Identifier(c)) for c, _ in replica_identity_cols_types)
        query += SQL("));")
        cur.execute(query)


def _generate_where_clause(mountpoint, table, cols, table_2=None):
    if not table_2:
        return SQL(" AND ").join(SQL("{}.{}.{} = %s").format(
            Identifier(mountpoint), Identifier(table), Identifier(c)) for c in cols)
    else:
        return SQL(" AND ").join(SQL("{}.{}.{} = {}.{}.{}").format(
            Identifier(mountpoint), Identifier(table), Identifier(c),
            Identifier(mountpoint), Identifier(table_2), Identifier(c)) for c in cols)


def _get_wal_change(conn, mountpoint, table, ri_cols, ri_vals):
    with conn.cursor() as cur:
        cur.execute(SQL("SELECT sg_action_kind, sg_action_data FROM {}.{} WHERE ").format(
            Identifier(mountpoint), Identifier(table)) + \
                    _generate_where_clause(mountpoint, table, ri_cols), ri_vals)
        return cur.fetchone()


def _add_wal_change(conn, mountpoint, table, ri_vals, kind, data):
    with conn.cursor() as cur:
        to_insert = ri_vals + [kind, data]
        cur.execute(SQL("INSERT INTO {}.{} ").format(
            Identifier(mountpoint), Identifier(table)) + \
                    SQL("VALUES (" + ','.join('%s' for _ in range(len(to_insert))) + ")"), to_insert)


def _update_wal_change(conn, mountpoint, table, ri_cols, ri_vals, new_kind, new_data):
    with conn.cursor() as cur:
        cur.execute(SQL("UPDATE {}.{} SET sg_action_kind = %s, sg_action_data = %s WHERE ").format(
            Identifier(mountpoint), Identifier(table)) + \
                    _generate_where_clause(mountpoint, table, ri_cols), [new_kind, new_data] + ri_vals)


def _delete_wal_change(conn, mountpoint, table, ri_cols, ri_vals):
    with conn.cursor() as cur:
        cur.execute(SQL("DELETE FROM {}.{} WHERE ").format(Identifier(mountpoint), Identifier(table)) + \
                    _generate_where_clause(mountpoint, table, ri_cols), ri_vals)


def _split_ri_cols(kind, change, ri_cols):
    # Returns 3 lists from the wal2json-produced change:
    # * ri_vals: values identifying the replica identity (RI) of a given tuple (matching column names in ri_cols)
    # * non_ri_cols: column names not in the RI that have been changed/updated
    # * non_ri_vals: column values not in the RI that have been changed/updated (matching colnames in non_ri_cols)
    non_ri_cols = []
    non_ri_vals = []
    ri_vals = []

    if kind == 0:
        for cc, cv in zip(change['columnnames'], change['columnvalues']):
            if cc in ri_cols:
                ri_vals.append(cv)
            else:
                non_ri_cols.append(cc)
                non_ri_vals.append(cv)
    elif kind == 1:
        for cc, cv in zip(change['oldkeys']['keynames'], change['oldkeys']['keyvalues']):
            if cc in ri_cols:
                ri_vals.append(cv)
    elif kind == 2:
        for cc, cv in zip(change['oldkeys']['keynames'], change['oldkeys']['keyvalues']):
            if cc in ri_cols:
                ri_vals.append(cv)
        for cc, cv in zip(change['columnnames'], change['columnvalues']):
            # Hmm: these might intersect with the RI values (e.g. when the whole tuple is the replica identity and
            # we're updating some of it)
            non_ri_cols.append(cc)
            non_ri_vals.append(cv)

    return ri_vals, non_ri_cols, non_ri_vals
    # todo raise here if not all ri_vals extracted


def _recalculate_disjoint_ri_cols(ri_cols, ri_vals, non_ri_cols, non_ri_vals):
    # Move the intersecting columns from the non-ri to the ri set
    new_nric = []
    new_nriv = []
    for nrc, nrv in zip(non_ri_cols, non_ri_vals):
        try:
            ri_vals[ri_cols.index(nrc)] = nrv
        except ValueError:
            new_nric.append(nrc)
            new_nriv.append(nrv)
    return ri_vals, new_nric, new_nriv


def _merge_wal_changes(old_changeset, new_keys, new_values):
    old_changeset = {k: v for k, v in zip(old_changeset['c'], old_changeset['v'])}
    old_changeset.update({k: v for k, v in zip(new_keys, new_values)})
    return {'c': list(old_changeset.keys()), 'v': list(old_changeset.values())}


def _convert_wal_change(kind, change, ri_cols):
    # Converts a change written by wal2json into our format:
    # (pk, kind, extra data)
    ri_vals, non_ri_cols, non_ri_vals = _split_ri_cols(kind, change, ri_cols)
    return tuple(ri_vals), kind, {'c': non_ri_cols, 'v': non_ri_vals} if kind == 0 or kind == 2 else None


def _pack_wal_change(conn, mountpoint, object_id, kind, change, ri_cols):
    ri_vals, non_ri_cols, non_ri_vals = _split_ri_cols(kind, change, ri_cols)
    old_change = _get_wal_change(conn, mountpoint, object_id, ri_cols, ri_vals)

    if kind == 0:  # Insert
        if old_change is None:
            # do we need to explicitly specify column names here?
            _add_wal_change(conn, mountpoint, object_id, ri_vals, kind=0, data=json.dumps({'c': non_ri_cols,
                                                                                           'v': non_ri_vals}))
        elif old_change[0] == 1:  # Insert over delete: change to update
            if not non_ri_cols and not non_ri_vals:
                # ...unless the full tuple is the PK, in which case we just pretend nothing has been deleted.
                _delete_wal_change(conn, mountpoint, object_id, ri_cols, ri_vals)
            else:
                _update_wal_change(conn, mountpoint, object_id, ri_cols, ri_vals, new_kind=2, new_data=json.dumps(
                    {'c': non_ri_cols,
                     'v': non_ri_vals}))
        else:
            raise SplitGraphException("TODO logic error")
    elif kind == 1:  # Delete
        if old_change is None:
            _add_wal_change(conn, mountpoint, object_id, ri_vals, kind=1, data=None)
        elif old_change[0] == 0 or old_change[0] == 2:
            # Remove a previous update/insert
            _delete_wal_change(conn, mountpoint, object_id, ri_cols, ri_vals)
        else:
            raise SplitGraphException("TODO logic error")
    else:  # Update
        # A bit of a corner case here: if the PK itself has been changed (normally impossible but can happen
        # if the tuple has no replica identity and so we use the whole of it as the PK), we record
        # the update as a delete + insert instead.
        pk_changed = any(c in ri_cols for c in non_ri_cols)

        if old_change is None:
            if pk_changed:
                _add_wal_change(conn, mountpoint, object_id, ri_vals, kind=1, data=None)
                ri_vals, non_ri_cols, non_ri_vals = _recalculate_disjoint_ri_cols(ri_cols, ri_vals, non_ri_cols, non_ri_vals)
                _add_wal_change(conn, mountpoint, object_id, ri_vals, kind=0, data=json.dumps({'c': non_ri_cols,
                                                                                               'v': non_ri_vals}))
            else:
                _add_wal_change(conn, mountpoint, object_id, ri_vals, kind=2, data=json.dumps({'c': non_ri_cols,
                                                                                               'v': non_ri_vals}))
        else:
            if pk_changed:
                _delete_wal_change(conn, mountpoint, object_id, ri_cols, ri_vals)
                if old_change[0] == 0:
                    # If we changed the PK after inserting, it's the same as just inserting the changed item
                    # with the new PK.
                    new_data = _merge_wal_changes(json.loads(old_change[1]), non_ri_cols, non_ri_vals)
                    ri_vals, non_ri_cols, non_ri_vals = _recalculate_disjoint_ri_cols(ri_cols, ri_vals, new_data['c'], new_data['v'])

                    # Here, the item we're writing into might already exist in the diff. The only way that doesn't
                    # violate PK constraints at the point when this change is made is if the item is market as deleted
                    # by this point.
                    old_change = _get_wal_change(conn, mountpoint, object_id, ri_cols, ri_vals)
                    if old_change is not None:
                        assert old_change[0] == 1
                        # This code is copied from the insert-over-update case.
                        if not non_ri_cols and not non_ri_vals:
                            _delete_wal_change(conn, mountpoint, object_id, ri_cols, ri_vals)
                        else:
                            _update_wal_change(conn, mountpoint, object_id, ri_cols, ri_vals, new_kind=2,
                                               new_data=json.dumps(
                                                   {'c': non_ri_cols,
                                                    'v': non_ri_vals}))
                    else:
                        _add_wal_change(conn, mountpoint, object_id, ri_vals, kind=0, data=json.dumps({'c': non_ri_cols,
                                                                                                       'v': non_ri_vals}))
                elif old_change[0] == 2:
                    # A normal update that didn't touch the PK, followed by a pk-touching update.
                    # Same as deleting the old item and inserting the new, updated item with the changed PK.
                    _add_wal_change(conn, mountpoint, object_id, ri_vals, kind=1, data=None)
                    new_data = _merge_wal_changes(json.loads(old_change[1]), non_ri_cols, non_ri_vals)
                    ri_vals, non_ri_cols, non_ri_vals = _recalculate_disjoint_ri_cols(ri_cols, ri_vals, new_data['c'],
                                                                                      new_data['v'])
                    _add_wal_change(conn, mountpoint, object_id, ri_vals, kind=0, data=json.dumps({'c': non_ri_cols,
                                                                                                   'v': non_ri_vals}))
            else:
                # Otherwise, simply update the previous update/insert.
                _update_wal_change(conn, mountpoint, object_id, ri_cols, ri_vals, new_kind=old_change[0], new_data=json.dumps(
                    _merge_wal_changes(json.loads(old_change[1]), non_ri_cols, non_ri_vals)))


def commit_pending_changes(conn, mountpoint, HEAD, new_image, include_snap=False):
    all_tables = get_all_tables(conn, mountpoint)
    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT DISTINCT(table_name) FROM {}.{}
                       WHERE mountpoint = %s""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                        Identifier("pending_changes")), (mountpoint,))
        changed_tables = [c[0] for c in cur.fetchall()]
        for table in all_tables:
            table_info = get_table(conn, mountpoint, table, HEAD)
            # Table already exists at the current HEAD
            if table_info:
                if table in changed_tables:
                    object_id = get_random_object_id()
                    repl_id = _get_replica_identity(conn, mountpoint, table)
                    ri_cols, ri_types = zip(*repl_id)
                    _create_diff_table(conn, mountpoint, object_id, repl_id)

                    with conn.cursor() as cur:
                        # Can't seem to use a server-side cursor here since it doesn't support DELETE FROM RETURNING
                        cur.execute(
                            SQL("""DELETE FROM {}.{} WHERE mountpoint = %s AND table_name = %s
                                   RETURNING kind, change """).format(
                                Identifier(SPLITGRAPH_META_SCHEMA), Identifier("pending_changes")), (mountpoint, table))

                        for kind, change in cur:
                            _pack_wal_change(conn, mountpoint, object_id, kind, json.loads(change), ri_cols)

                        cur.execute(SQL("SELECT COUNT(1) FROM {}.{}").format(Identifier(mountpoint), Identifier(object_id)))
                        if cur.fetchone()[0] == 0:
                            # Changes in the WAL cancelled each other out. Delete the diff table and just point
                            # the commit to the old table objects.
                            cur.execute(
                                SQL("DROP TABLE {}.{}").format(Identifier(mountpoint), Identifier(object_id)))
                            for prev_object_id, prev_format in table_info:
                                register_table_object(conn, mountpoint, table, new_image, prev_object_id, prev_format)
                        else:
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
                        cur.execute(SQL("CREATE TABLE {}.{} AS SELECT * FROM {}.{}").format(
                            Identifier(mountpoint), Identifier(object_id),
                            Identifier(mountpoint), Identifier(table)))
                        register_table_object(conn, mountpoint, table, new_image, object_id, object_format='SNAP')

            # Finally, if the table was created (only snap was output), drop all of its WAL changes since we
            # don't want them incorporated in the next commit.
            if not table_info:
                discard_pending_changes(conn, mountpoint)


def apply_record_to_staging(conn, mountpoint, object_id, destination):
    queries = []
    repl_id = _get_replica_identity(conn, mountpoint, object_id)
    ri_cols, ri_types = zip(*repl_id)

    # Minor hack alert: here we assume that the PK of the object is the PK of the table it refers to, which means
    # that we are expected to have the PKs applied to the object table no matter how it originated.
    if sorted(ri_cols) == sorted(_get_column_names(conn, mountpoint, object_id)):
        raise SplitGraphException("Error determining the replica identity of %s. " % object_id +\
                                  "Have primary key constrants been applied?")

    with conn.cursor() as cur:
        # Apply deletes
        cur.execute(SQL("DELETE FROM {0}.{1} USING {0}.{2} WHERE {0}.{2}.sg_action_kind = 1").format(
            Identifier(mountpoint), Identifier(destination), Identifier(object_id)) +\
                    SQL(" AND ") + _generate_where_clause(mountpoint, destination, ri_cols, object_id))

        # Generate queries for inserts
        cur.execute(SQL("SELECT * FROM {}.{} WHERE sg_action_kind = 0").format(Identifier(mountpoint), Identifier(object_id)))
        for row in cur:
            # Not sure if we can rely on ordering here.
            # Also for the future: if all column names are the same, we can do a big INSERT.
            action_data = json.loads(row[-1])
            cols_to_insert = list(ri_cols) + action_data['c']
            vals_to_insert = list(row[:-2]) + action_data['v']

            query = SQL("INSERT INTO {}.{} (").format(Identifier(mountpoint), Identifier(destination))
            query += SQL(','.join('{}' for _ in cols_to_insert)).format(*[Identifier(c) for c in cols_to_insert])
            query += SQL(") VALUES (" + ','.join('%s' for _ in vals_to_insert) + ')')
            query = cur.mogrify(query, vals_to_insert)
            queries.append(query)
        # ...and updates
        cur.execute(
            SQL("SELECT * FROM {}.{} WHERE sg_action_kind = 2").format(Identifier(mountpoint), Identifier(object_id)))
        for row in cur:
            action_data = json.loads(row[-1])
            ri_vals = list(row[:-2])
            cols_to_insert = action_data['c']
            vals_to_insert = action_data['v']

            query = SQL("UPDATE {}.{} SET ").format(Identifier(mountpoint), Identifier(destination))
            query += SQL(', '.join("{} = %s" for _ in cols_to_insert)).format(*(Identifier(i) for i in cols_to_insert))
            query += SQL(" WHERE ") + _generate_where_clause(mountpoint, destination, ri_cols)
            queries.append(cur.mogrify(query, vals_to_insert + ri_vals))
        # Apply the insert/update queries (might not exist if the diff was all deletes)
        if queries:
            cur.execute(b';'.join(queries))  # maybe some pagination needed here.


def _get_column_names(conn, mountpoint, table_name):
    with conn.cursor() as cur:
        cur.execute("""SELECT column_name FROM information_schema.columns
                       WHERE table_schema = %s
                       AND table_name = %s""", (mountpoint, table_name))
        return [c[0] for c in cur.fetchall()]


def _get_column_names_types(conn, mountpoint, table_name):
    with conn.cursor() as cur:
        cur.execute("""SELECT column_name, data_type FROM information_schema.columns
                       WHERE table_schema = %s
                       AND table_name = %s""", (mountpoint, table_name))
        return cur.fetchall()