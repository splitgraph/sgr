import json

from psycopg2.extras import execute_batch, Json
from psycopg2.sql import SQL, Identifier

from splitgraph.constants import get_random_object_id, SPLITGRAPH_META_SCHEMA, SplitGraphException
from splitgraph.meta_handler import get_all_tables, get_table, register_table, ensure_metadata_schema
from splitgraph.meta_handler import get_table_with_format, register_object, \
    get_object_parents, get_object_format
from splitgraph.pg_audit import manage_audit_triggers, discard_pending_changes
from splitgraph.pg_utils import copy_table, get_primary_keys, _get_column_names, _get_column_names_types, \
    get_full_table_schema


def manage_audit(func):
    # A decorator to be put around various SG commands that performs general admin and auditing management
    # (makes sure the metadata schema exists and delete/add required audit triggers)
    def wrapped(*args, **kwargs):
        conn = args[0]
        try:
            ensure_metadata_schema(conn)
            manage_audit_triggers(conn)
            func(*args, **kwargs)
        finally:
            conn.commit()
            manage_audit_triggers(conn)

    return wrapped


# "Truncate" kind is missing
KIND = {'insert': 0, 'delete': 1, 'update': 2}
AUDIT_KIND = {'I': 0, 'D': 1, 'U': 2}


def dump_pending_changes(conn, mountpoint, table, aggregate=False):
    # First, make sure we're up to date on changes.
    with conn.cursor() as cur:
        if aggregate:
            cur.execute(SQL(
                "SELECT action, count(action) FROM {}.{} "
                "WHERE schema_name = %s AND table_name = %s GROUP BY action").format(Identifier("audit"),
                                                                                     Identifier("logged_actions")),
                        (mountpoint, table))
            return [(AUDIT_KIND[k], c) for k, c in cur.fetchall()]

        cur.execute(SQL(
            "SELECT action, row_data, changed_fields FROM {}.{} "
            "WHERE schema_name = %s AND table_name = %s").format(Identifier("audit"),
                                                                 Identifier("logged_actions")),
                    (mountpoint, table))
        repl_id = _get_replica_identity(conn, mountpoint, table)
        ri_cols, _ = zip(*repl_id)
        result = []
        for action, row_data, changed_fields in cur:
            result.extend(_convert_audit_change(action, row_data, changed_fields, ri_cols))
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
        cur.execute(query)
        # RI is PK anyway, so has an index by default


def _generate_where_clause(mountpoint, table, cols, table_2=None, mountpoint_2=None):
    if not table_2:
        return SQL(" AND ").join(SQL("{}.{}.{} = %s").format(
            Identifier(mountpoint), Identifier(table), Identifier(c)) for c in cols)
    return SQL(" AND ").join(SQL("{}.{}.{} = {}.{}.{}").format(
        Identifier(mountpoint), Identifier(table), Identifier(c),
        Identifier(mountpoint_2), Identifier(table_2), Identifier(c)) for c in cols)


def _split_ri_cols(action, row_data, changed_fields, ri_cols):
    """
    :return: `(ri_vals, non_ri_cols, non_ri_vals)`: a tuple of 3 lists:
        * `ri_vals`: values identifying the replica identity (RI) of a given tuple (matching column names in `ri_cols`)
        * `non_ri_cols`: column names not in the RI that have been changed/updated
        * `non_ri_vals`: column values not in the RI that have been changed/updated (matching colnames in `non_ri_cols`)
    """
    non_ri_cols = []
    non_ri_vals = []
    ri_vals = [None] * len(ri_cols)

    if action == 'I':
        for cc, cv in row_data.items():
            if cc in ri_cols:
                ri_vals[ri_cols.index(cc)] = cv
            else:
                non_ri_cols.append(cc)
                non_ri_vals.append(cv)
    elif action == 'D':
        for cc, cv in row_data.items():
            if cc in ri_cols:
                ri_vals[ri_cols.index(cc)] = cv
    elif action == 'U':
        for cc, cv in row_data.items():
            if cc in ri_cols:
                ri_vals[ri_cols.index(cc)] = cv
        for cc, cv in changed_fields.items():
            # Hmm: these might intersect with the RI values (e.g. when the whole tuple is the replica identity and
            # we're updating some of it)
            non_ri_cols.append(cc)
            non_ri_vals.append(cv)

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


def merge_changes(old_change_data, new_change_data):
    old_change_data = {k: v for k, v in zip(old_change_data['c'], old_change_data['v'])}
    old_change_data.update({k: v for k, v in zip(new_change_data['c'], new_change_data['v'])})
    return {'c': list(old_change_data.keys()), 'v': list(old_change_data.values())}


def _convert_audit_change(action, row_data, changed_fields, ri_cols):
    """
    Converts the audit log entry into our internal format.
    :returns: [(pk, kind, extra data)] (more than 1 change might be emitted from a single audit entry).
    """
    ri_vals, non_ri_cols, non_ri_vals = _split_ri_cols(action, row_data, changed_fields, ri_cols)
    pk_changed = any(c in ri_cols for c in non_ri_cols)
    if pk_changed:
        assert action == 'U'
        # If it's an update that changed the PK (e.g. the table has no replica identity so we treat the whole
        # tuple as a primary key), then we turn it into a delete old tuple + insert new one.
        # This might happen with updates in any case, since the WAL seems to output the old and the new values for the
        # PK no matter what the replica identity settings are. However, the resulting delete + insert
        # gets conflated back into an update in any case if the PK is the same between the two.
        result = [(tuple(ri_vals), 1, None)]
        # todo: will this work if a part of the primary key + some other column in the tuple has been updated?
        ri_vals, non_ri_cols, non_ri_vals = _recalculate_disjoint_ri_cols(ri_cols, ri_vals, non_ri_cols, non_ri_vals)
        result.append((tuple(ri_vals), 0, {'c': non_ri_cols, 'v': non_ri_vals}))
        return result
    return [(tuple(ri_vals), AUDIT_KIND[action],
             {'c': non_ri_cols, 'v': non_ri_vals} if action in ('I', 'U') else None)]


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
                if old_change[0] == 0 or old_change[0] == 2:
                    new_data = merge_changes(old_change[1], change_data)
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
    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT DISTINCT(table_name) FROM {}.{}
                       WHERE schema_name = %s""").format(Identifier("audit"),
                                                         Identifier("logged_actions")), (mountpoint,))
        changed_tables = [c[0] for c in cur.fetchall()]
    for table in get_all_tables(conn, mountpoint):
        table_info = get_table(conn, mountpoint, table, current_head)
        # Table already exists at the current HEAD
        if table_info:
            # If there has been a schema change, we currently just snapshot the whole table.
            # This is obviously wasteful (say if just one column has been added/dropped or we added a PK,
            # but it's a starting point to support schema changes.
            if table_schema_changed(conn, mountpoint, table, image_1=current_head, image_2=None):
                _record_table_as_snap(conn, mountpoint, image_hash, table, table_info)
                continue

            if table in changed_tables:
                _record_table_as_diff(conn, mountpoint, image_hash, table, table_info)
            else:
                # If the table wasn't changed, point the commit to the old table objects (including
                # any of snaps or diffs).
                # This feels slightly weird: are we denormalized here?
                for prev_object_id, _ in table_info:
                    register_table(conn, mountpoint, table, image_hash, prev_object_id)

        # If table created (or we want to store a snap anyway), copy the whole table over as well.
        if not table_info or include_snap:
            _record_table_as_snap(conn, mountpoint, image_hash, table, table_info)

    # Make sure that all pending changes have been discarded by this point (e.g. if we created just a snapshot for
    # some tables and didn't consume the WAL).
    # NB if we allow partial commits, this will have to be changed (only discard for committed tables).
    discard_pending_changes(conn, mountpoint)


def _record_table_as_snap(conn, mountpoint, image_hash, table, table_info):
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


def _record_table_as_diff(conn, mountpoint, image_hash, table, table_info):
    object_id = get_random_object_id()
    repl_id = _get_replica_identity(conn, mountpoint, table)
    ri_cols, _ = zip(*repl_id)
    _create_diff_table(conn, object_id, repl_id)
    with conn.cursor() as cur:
        # Can't seem to use a server-side cursor here since it doesn't support DELETE FROM RETURNING
        cur.execute(
            SQL("""DELETE FROM {}.{} WHERE schema_name = %s AND table_name = %s
                                   RETURNING action, row_data, changed_fields""").format(
                Identifier("audit"), Identifier("logged_actions")), (mountpoint, table))
        # Accumulate the diff in-memory.
        changeset = {}
        for action, row_data, changed_fields in cur:
            _conflate_changes(changeset, _convert_audit_change(action, row_data, changed_fields, ri_cols))

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


def _convert_vals(vals):
    """Psycopg returns jsonb objects as dicts but doesn't actually accept them directly
    as a query param. Hence, we have to wrap them in the Json datatype when applying a DIFF
    to a table."""
    # This might become a bottleneck since we call this for every row in the diff + function
    # calls are expensive in Python -- maybe there's a better way (e.g. tell psycopg to not convert
    # things to dicts or apply diffs in-driver).
    return [v if not isinstance(v, dict) else Json(v) for v in vals]


def apply_record_to_staging(conn, object_id, mountpoint, destination):
    """
    Applies a DIFF table stored in `object_id` to destination.
    :param conn: psycopg connection object.
    :param object_id: Object ID of the DIFF table.
    :param mountpoint: Schema where the destination table is located.
    :param destination: Target table.
    """
    queries = []
    repl_id = _get_replica_identity(conn, SPLITGRAPH_META_SCHEMA, object_id)
    ri_cols, _ = zip(*repl_id)

    # Minor hack alert: here we assume that the PK of the object is the PK of the table it refers to, which means
    # that we are expected to have the PKs applied to the object table no matter how it originated.
    if sorted(ri_cols) == sorted(_get_column_names(conn, SPLITGRAPH_META_SCHEMA, object_id)):
        raise SplitGraphException("Error determining the replica identity of %s. " % object_id +
                                  "Have primary key constraints been applied?")

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
            vals_to_insert = _convert_vals(list(row[:-2]) + action_data['v'])

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
        return get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_1) != \
               get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_2)
    return get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_1) != \
           get_full_table_schema(conn, mountpoint, table_name)


def get_schema_at(conn, mountpoint, table_name, image_hash):
    snap_1 = get_closest_parent_snap_object(conn, mountpoint, table_name, image_hash)[0]
    return get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_1)


def get_closest_parent_snap_object(conn, mountpoint, table, image):
    path = []
    object_id = get_table_with_format(conn, mountpoint, table, image, object_format='SNAP')
    if object_id is not None:
        return object_id, path

    object_id = get_table_with_format(conn, mountpoint, table, image, object_format='DIFF')
    while object_id is not None:
        path.append(object_id)
        parents = get_object_parents(conn, object_id)
        for object_id in parents:
            if get_object_format(conn, object_id) == 'SNAP':
                return object_id, path
            break  # Found 1 diff, will be added to the path at the next iteration.

    # We didn't find an actual snapshot for this table -- either it doesn't exist in this
    # version or something went wrong. Should we raise here?
