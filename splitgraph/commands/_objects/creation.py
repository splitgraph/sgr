"""
Internal functions for packaging changes into physical Splitgraph objects.
"""

from psycopg2.extras import execute_batch, Json
from psycopg2.sql import SQL, Identifier

from splitgraph._data.objects import register_table, register_object, get_object_for_table
from splitgraph.commands._objects.utils import get_replica_identity, conflate_changes, convert_audit_change, \
    get_random_object_id
from splitgraph.commands.misc import delete_objects
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.connection import get_connection
from splitgraph.pg_utils import copy_table


def _create_diff_table(object_id, replica_identity_cols_types):
    """
    Create a diff table into which we'll pack the conflated audit log actions.

    :param object_id: table name to create
    :param replica_identity_cols_types: multiple columns forming the table's PK (or all rows), PKd.
    """
    # sg_action_kind: 0, 1, 2 for insert/delete/update
    # sg_action_data: extra data for insert and update.
    with get_connection().cursor() as cur:
        query = SQL("CREATE TABLE {}.{} (").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id))
        query += SQL(',').join(SQL("{} %s" % col_type).format(Identifier(col_name))
                               for col_name, col_type in replica_identity_cols_types + [('sg_action_kind', 'smallint'),
                                                                                        ('sg_action_data', 'jsonb')])
        query += SQL(", PRIMARY KEY (") + SQL(',').join(
            SQL("{}").format(Identifier(c)) for c, _ in replica_identity_cols_types)
        query += SQL("));")
        cur.execute(query)
        # RI is PK anyway, so has an index by default


def record_table_as_diff(repository, image_hash, table, table_info):
    """
    Flushes the pending changes from the audit table for a given table and records them, registering the new objects.
    :param repository: Repository object
    :param image_hash: Hash of the new image
    :param table: Table name
    :param table_info: Information for the previous version of this table (list of object IDs and their formats)
    """
    object_id = get_random_object_id()
    repl_id = get_replica_identity(repository.to_schema(), table)
    ri_cols, _ = zip(*repl_id)
    _create_diff_table(object_id, repl_id)

    changeset = _get_conflated_changeset(repository, table, ri_cols)

    if changeset:
        query = SQL("INSERT INTO {}.{} ").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                 Identifier(object_id)) + \
                SQL("VALUES (" + ','.join('%s' for _ in range(len(changeset[0]))) + ")")

        with get_connection().cursor() as cur:
            execute_batch(cur, query, changeset, page_size=1000)

        for parent_id, _ in table_info:
            register_object(object_id, object_format='DIFF', namespace=repository.namespace,
                            parent_object=parent_id)

        register_table(repository, table, image_hash, object_id)
    else:
        # Changes in the audit log cancelled each other out. Delete the diff table and just point
        # the commit to the old table objects.
        delete_objects([object_id])
        for prev_object_id, _ in table_info:
            register_table(repository, table, image_hash, prev_object_id)


def _get_conflated_changeset(repository, table, ri_cols):
    # Can't seem to use a server-side cursor here since it doesn't support DELETE FROM RETURNING
    with get_connection().cursor() as cur:
        cur.execute(
            SQL("""DELETE FROM {}.{} WHERE schema_name = %s AND table_name = %s
                                       RETURNING action, row_data, changed_fields""")
                .format(Identifier("audit"), Identifier("logged_actions")), (repository.to_schema(), table))

        # Accumulate the diff in-memory. This might become a bottleneck in the future.
        changeset = {}
        for action, row_data, changed_fields in cur:
            conflate_changes(changeset, convert_audit_change(action, row_data, changed_fields, ri_cols))
        return [tuple(list(pk) + [kind_data[0], Json(kind_data[1])]) for pk, kind_data in
                changeset.items()]


def record_table_as_snap(repository, image_hash, table, table_info):
    """
    Copies the full table verbatim into a new Splitgraph SNAP object, registering the new object.

    :param repository: Repository object
    :param image_hash: Hash of the new image
    :param table: Table name
    :param table_info: Information for the previous version of this table (list of object IDs and their formats)
    """
    # Make sure we didn't actually create a snap for this table.
    if get_object_for_table(repository, table, image_hash, 'SNAP') is None:
        object_id = get_random_object_id()
        copy_table(get_connection(), repository.to_schema(), table, SPLITGRAPH_META_SCHEMA, object_id,
                   with_pk_constraints=True)
        if table_info:
            for parent_id, _ in table_info:
                register_object(object_id, object_format='SNAP', namespace=repository.namespace,
                                parent_object=parent_id)
        else:
            register_object(object_id, object_format='SNAP', namespace=repository.namespace, parent_object=None)
        register_table(repository, table, image_hash, object_id)
