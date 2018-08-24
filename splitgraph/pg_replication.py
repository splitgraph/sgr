import json

from psycopg2.extras import execute_batch
from psycopg2.sql import SQL, Identifier

from splitgraph.constants import get_random_object_id, SPLITGRAPH_META_SCHEMA, SplitGraphException
from splitgraph.meta_handler import get_all_tables, get_table, register_table, ensure_metadata_schema
from splitgraph.meta_handler import get_current_mountpoints_hashes, get_table_with_format, register_object, \
    get_object_parents, get_object_format
from splitgraph.pg_utils import copy_table, get_primary_keys, _get_column_names, _get_column_names_types, \
    _get_full_table_schema

LOGICAL_DECODER = 'wal2json'
REPLICATION_SLOT = 'sg_replication'


def suspend_replication(func):
    # A decorator to be put around various SG commands that performs general admin and replication management
    # (makes sure the metadata schema exists and suspends replication so that commits/system bookkeeping doesn't get
    # reflected in the consumed logical replication stream).
    def wrapped(*args, **kwargs):
        conn = args[0]
        try:
            ensure_metadata_schema(conn)
            record_pending_changes(conn)
            stop_replication(conn)
            func(*args, **kwargs)
        finally:
            start_replication(conn)

    return wrapped


def replication_slot_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""SELECT 1 FROM pg_replication_slots WHERE slot_name = %s""", (REPLICATION_SLOT,))
        return cur.fetchone() is not None


def start_replication(conn):
    with conn.cursor() as cur:
        cur.execute("""SELECT 'init' FROM pg_create_logical_replication_slot(%s, %s)""",
                    (REPLICATION_SLOT, LOGICAL_DECODER))


def stop_replication(conn):
    if replication_slot_exists(conn):
        with conn.cursor() as cur:
            cur.execute("""SELECT 'stop' FROM pg_drop_replication_slot(%s)""", (REPLICATION_SLOT,))


def _consume_changes(conn):
    # Consuming changes means they won't be returned again from this slot. Perhaps worth doing peeking first
    # in case we crash after consumption but before recording changes.
    if not replication_slot_exists(conn):
        return []

    all_mountpoints = [m[0] for m in get_current_mountpoints_hashes(conn)]
    table_filter_param = ','.join('%s.*' % m for m in all_mountpoints)

    with conn.cursor() as cur:
        cur.execute("""SELECT data FROM pg_logical_slot_get_changes(%s, NULL, NULL, 'add-tables', %s)""",
                    (REPLICATION_SLOT, table_filter_param))

        return cur.fetchall()


KIND = {'insert': 0, 'delete': 1, 'update': 2}


def record_pending_changes(conn):
    # Decode the changeset produced by wal2json and save it to the pending_changes table.
    changes = _consume_changes(conn)
    if not changes:
        return
    # If the table doesn't exist in the current commit, we'll be storing it as a full snapshot,
    # so there's no point consuming the WAL for it.
    mountpoints_tables = {m: [t for t in get_all_tables(conn, m) if get_table(conn, m, t, head)]
                          for m, head in get_current_mountpoints_hashes(conn)}

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
    conn.commit()  # Slightly eww: a diff() also dumps pending changes, which means that if the diff
    # doesn't commit, the changes will have been consumed but not saved.


def discard_pending_changes(conn, mountpoint):
    with conn.cursor() as cur:
        cur.execute(SQL("DELETE FROM {}.{} WHERE mountpoint = %s").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                          Identifier("pending_changes")),
                    (mountpoint,))
    conn.commit()


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
                result.extend(_convert_wal_change(kind, json.loads(change), ri_cols))
            return result


def _get_replica_identity(conn, mountpoint, table):
    return get_primary_keys(conn, mountpoint, table) or _get_column_names_types(conn, mountpoint, table)


def _create_diff_table(conn, object_id, replica_identity_cols_types):
    """
    Create a diff table into which we'll pack the conflated WAL actions.
    :param conn: psycopg connection object
    :param object_id: table name to create
    :param replica_identity_cols_types: multiple columns forming the table's PK (or all rows), PKd.
    """
    # sg_action_kind: 0, 1, 2 for insert/delete/update
    # sg_action_data: extra data for insert and update.
    with conn.cursor() as cur:
        query = SQL("CREATE TABLE {}.{} (").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id))
        query += SQL(',').join(SQL("{} %s" % col_type).format(Identifier(col_name))
                               for col_name, col_type in replica_identity_cols_types + [('sg_action_kind', 'smallint'),
                                                                                        ('sg_action_data', 'varchar')])
        query += SQL(", PRIMARY KEY (") + SQL(',').join(
            SQL("{}").format(Identifier(c)) for c, _ in replica_identity_cols_types)
        query += SQL("));")

        # Also create an index on the replica identity since we'll be querying that to conflate changes.
        query += SQL("CREATE INDEX ON {}.{} (").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)) + \
                 SQL(',').join(SQL("{}").format(Identifier(c)) for c, _ in replica_identity_cols_types) + SQL(');')
        cur.execute(query)


def _generate_where_clause(mountpoint, table, cols, table_2=None, mountpoint_2=None):
    if not table_2:
        return SQL(" AND ").join(SQL("{}.{}.{} = %s").format(
            Identifier(mountpoint), Identifier(table), Identifier(c)) for c in cols)
    else:
        return SQL(" AND ").join(SQL("{}.{}.{} = {}.{}.{}").format(
            Identifier(mountpoint), Identifier(table), Identifier(c),
            Identifier(mountpoint_2), Identifier(table_2), Identifier(c)) for c in cols)


def _split_ri_cols(kind, change, ri_cols):
    """
    :return: `(ri_vals, non_ri_cols, non_ri_vals)`: a tuple of 3 lists:
        * `ri_vals`: values identifying the replica identity (RI) of a given tuple (matching column names in `ri_cols`)
        * `non_ri_cols`: column names not in the RI that have been changed/updated
        * `non_ri_vals`: column values not in the RI that have been changed/updated (matching colnames in `non_ri_cols`)
    """
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

    if len(ri_vals) != len(ri_cols):
        raise SplitGraphException(
            "Not all replica identity columns (of %r) were extracted from change %r!" % (ri_cols, change))
    return ri_vals, non_ri_cols, non_ri_vals


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


def _merge_wal_changes(old_change_data, new_change_data):
    old_change_data = {k: v for k, v in zip(old_change_data['c'], old_change_data['v'])}
    old_change_data.update({k: v for k, v in zip(new_change_data['c'], new_change_data['v'])})
    return {'c': list(old_change_data.keys()), 'v': list(old_change_data.values())}


def _convert_wal_change(kind, change, ri_cols):
    """
    Converts a change written by wal2json into our internal format:
    :returns: [(pk, kind, extra data)] (more than 1 change might be emitted from a single WAL entry).
    """
    ri_vals, non_ri_cols, non_ri_vals = _split_ri_cols(kind, change, ri_cols)
    pk_changed = any(c in ri_cols for c in non_ri_cols)
    if pk_changed:
        assert kind == 2
        # If it's an update that changed the PK (e.g. the table has no replica identity so we treat the whole
        # tuple as a primary key), then we turn it into a delete old tuple + insert new one.
        result = [(tuple(ri_vals), 1, None)]
        # todo: will this work if a part of the primary key + some other column in the tuple has been updated?
        ri_vals, non_ri_cols, non_ri_vals = _recalculate_disjoint_ri_cols(ri_cols, ri_vals, non_ri_cols, non_ri_vals)
        result.append((tuple(ri_vals), 0, {'c': non_ri_cols, 'v': non_ri_vals}))
        return result
    else:
        return [(tuple(ri_vals), kind, {'c': non_ri_cols, 'v': non_ri_vals} if kind == 0 or kind == 2 else None)]


def _conflate_changes(changeset, new_changes):
    """
    Updates a changeset to incorporate the new changes. Assumes that the new changes are non-pk changing
    (e.g. PK-changing updates have been converted into a del + ins).
    """
    for change_pk, change_kind, change_data in new_changes:
        old_change = changeset.get(change_pk)
        if not old_change:
            changeset[change_pk] = (change_kind, change_data)
        else:
            if change_kind == 0:
                if old_change[0] == 1:  # Insert over delete: change to update
                    if change_data == {'c': [], 'v': []}:
                        del changeset[change_pk]
                    else:
                        changeset[change_pk] = (2, change_data)
                else:
                    raise SplitGraphException("TODO logic error")
            elif change_kind == 1:  # Delete over insert/update: remove the old change
                del changeset[change_pk]
                if old_change[0] == 2:
                    # If it was an update, also remove the old row.
                    changeset[change_pk] = (1, change_data)
                if old_change[0] == 1:
                    # Delete over delete: can't happen.
                    raise SplitGraphException("TODO logic error")
            elif change_kind == 2:  # Update over insert/update: merge the two changes.
                if old_change[0] == 0 or old_change[0] == 1:
                    new_data = _merge_wal_changes(json.loads(old_change[1]), change_data)
                    changeset[change_pk] = (old_change[0], new_data)


def commit_pending_changes(conn, mountpoint, current_head, image_hash, include_snap=False):
    """
    Reads the recorded pending changes to all tables in a given mountpoint, conflates them and possibly stores them
    as new object(s) as follows:
        * If a table has been created or there has been a schema change, it's only stored as a SNAP (full snapshot).
        * If a table hasn't changed since the last revision, no new objects are created and it's linked to the previous
          objects belonging to the last revision.
        * Otherwise, the table is stored as a conflated (1 change per PK) DIFF object and an optional SNAP.
    :param conn: psycopg connection object.
    :param mountpoint: Mountpoint to commit.
    :param current_head: Current HEAD pointer to base the commit on.
    :param image_hash: Hash of the image to commit changes under.
    :param include_snap: If True, also stores the table as a SNAP.
    """
    all_tables = get_all_tables(conn, mountpoint)
    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT DISTINCT(table_name) FROM {}.{}
                       WHERE mountpoint = %s""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                        Identifier("pending_changes")), (mountpoint,))
        changed_tables = [c[0] for c in cur.fetchall()]
        for table in all_tables:
            table_info = get_table(conn, mountpoint, table, current_head)
            # Table already exists at the current HEAD
            if table_info:
                schema_changed = table_schema_changed(conn, mountpoint, table, image_1=current_head, image_2=None)
                # If there has been a schema change, we currently just snapshot the whole table.
                # This is obviously wasteful (say if just one column has been added/dropped or we added a PK,
                # but it's a starting point to support schema changes.
                if schema_changed:
                    include_snap = True
                if table in changed_tables and not schema_changed:
                    object_id = get_random_object_id()
                    repl_id = _get_replica_identity(conn, mountpoint, table)
                    ri_cols, ri_types = zip(*repl_id)
                    _create_diff_table(conn, object_id, repl_id)

                    with conn.cursor() as cur:
                        # Can't seem to use a server-side cursor here since it doesn't support DELETE FROM RETURNING
                        cur.execute(
                            SQL("""DELETE FROM {}.{} WHERE mountpoint = %s AND table_name = %s
                                   RETURNING kind, change """).format(
                                Identifier(SPLITGRAPH_META_SCHEMA), Identifier("pending_changes")), (mountpoint, table))

                        # Accumulate the diff in-memory.
                        changeset = {}
                        for kind, change in cur:
                            _conflate_changes(changeset, _convert_wal_change(kind, json.loads(change), ri_cols))

                        if changeset:
                            changeset = [tuple(list(pk) + [kind_data[0], json.dumps(kind_data[1])]) for pk, kind_data in
                                         changeset.items()]
                            query = SQL("INSERT INTO {}.{} ").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                     Identifier(object_id)) + \
                                    SQL("VALUES (" + ','.join('%s' for _ in range(len(changeset[0]))) + ")")
                            execute_batch(cur, query, changeset, page_size=1000)

                            for parent_id, _ in table_info:
                                register_object(conn, object_id, object_format='DIFF', parent_object=parent_id)
                            register_table(conn, mountpoint, table, image_hash, object_id)
                        else:
                            # Changes in the WAL cancelled each other out. Delete the diff table and just point
                            # the commit to the old table objects.
                            cur.execute(
                                SQL("DROP TABLE {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                               Identifier(object_id)))
                            for prev_object_id, _ in table_info:
                                register_table(conn, mountpoint, table, image_hash, prev_object_id)
                else:
                    if not schema_changed:
                        # If the table wasn't changed, point the commit to the old table objects (including
                        # any of snaps or diffs).
                        # This feels slightly weird: are we denormalized here?
                        for prev_object_id, _ in table_info:
                            register_table(conn, mountpoint, table, image_hash, prev_object_id)

            # If table created (or we want to store a snap anyway), copy the whole table over as well.
            if not table_info or include_snap:
                # Make sure we didn't actually create a snap for this table.
                if get_table_with_format(conn, mountpoint, table, image_hash, 'SNAP') is None:
                    object_id = get_random_object_id()
                    copy_table(conn, mountpoint, table, SPLITGRAPH_META_SCHEMA, object_id, with_pk_constraints=True)
                    if table_info:
                        for parent_id, _ in table_info:
                            register_object(conn, object_id, object_format='SNAP', parent_object=parent_id)
                    else:
                        register_object(conn, object_id, object_format='SNAP', parent_object=None)
                    register_table(conn, mountpoint, table, image_hash, object_id)

    # Make sure that all pending changes have been discarded by this point (e.g. if we created just a snapshot for
    # some tables and didn't consume the WAL).
    # NB if we allow partial commits, this will have to be changed (only discard for committed tables).
    discard_pending_changes(conn, mountpoint)


def apply_record_to_staging(conn, object_id, mountpoint, destination):
    queries = []
    repl_id = _get_replica_identity(conn, SPLITGRAPH_META_SCHEMA, object_id)
    ri_cols, ri_types = zip(*repl_id)

    # Minor hack alert: here we assume that the PK of the object is the PK of the table it refers to, which means
    # that we are expected to have the PKs applied to the object table no matter how it originated.
    if sorted(ri_cols) == sorted(_get_column_names(conn, SPLITGRAPH_META_SCHEMA, object_id)):
        raise SplitGraphException("Error determining the replica identity of %s. " % object_id +
                                  "Have primary key constrants been applied?")

    with conn.cursor() as cur:
        # Apply deletes
        cur.execute(SQL("DELETE FROM {0}.{2} USING {1}.{3} WHERE {1}.{3}.sg_action_kind = 1").format(
            Identifier(mountpoint), Identifier(SPLITGRAPH_META_SCHEMA), Identifier(destination),
            Identifier(object_id)) + SQL(" AND ") + _generate_where_clause(mountpoint, destination, ri_cols, object_id,
                                                                           SPLITGRAPH_META_SCHEMA))

        # Generate queries for inserts
        cur.execute(
            SQL("SELECT * FROM {}.{} WHERE sg_action_kind = 0").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                       Identifier(object_id)))
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
            SQL("SELECT * FROM {}.{} WHERE sg_action_kind = 2").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                       Identifier(object_id)))
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


def table_schema_changed(conn, mountpoint, table_name, image_1, image_2=None):
    snap_1 = get_closest_parent_snap_object(conn, mountpoint, table_name, image_1)[0]
    # image_2 = None here means the current staging area.
    if image_2 is not None:
        snap_2 = get_closest_parent_snap_object(conn, mountpoint, table_name, image_2)[0]
        return _get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_1) != \
               _get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_2)
    else:
        return _get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_1) != \
               _get_full_table_schema(conn, mountpoint, table_name)


def get_closest_parent_snap_object(conn, mountpoint, table, image):
    path = []
    object_id = get_table_with_format(conn, mountpoint, table, image, object_format='SNAP')
    if object_id is not None:
        return object_id, path
    else:
        object_id = get_table_with_format(conn, mountpoint, table, image, object_format='DIFF')

    while object_id is not None:
        path.append(object_id)
        parents = get_object_parents(conn, object_id)
        for object_id in parents:
            if get_object_format(conn, object_id) == 'SNAP':
                return object_id, path
            else:
                break  # Found 1 diff, will be added to the path at the next iteration.
    # We didn't find an actual snapshot for this table -- either it doesn't exist in this
    # version or something went wrong. Should we raise here?
