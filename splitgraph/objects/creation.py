import json

from psycopg2.extras import execute_batch, Json
from psycopg2.sql import SQL, Identifier

from splitgraph.connection import get_connection
from splitgraph.constants import SPLITGRAPH_META_SCHEMA, get_random_object_id
from splitgraph.meta_handler.objects import register_table, register_object
from splitgraph.meta_handler.tables import get_object_for_table
from splitgraph.objects.utils import get_replica_identity, conflate_changes, convert_audit_change
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
                                                                                        ('sg_action_data', 'varchar')])
        query += SQL(", PRIMARY KEY (") + SQL(',').join(
            SQL("{}").format(Identifier(c)) for c, _ in replica_identity_cols_types)
        query += SQL("));")
        cur.execute(query)
        # RI is PK anyway, so has an index by default


def record_table_as_diff(repository, image_hash, table, table_info):
    object_id = get_random_object_id()
    repl_id = get_replica_identity(repository.to_schema(), table)
    ri_cols, _ = zip(*repl_id)
    _create_diff_table(object_id, repl_id)
    with get_connection().cursor() as cur:
        # Can't seem to use a server-side cursor here since it doesn't support DELETE FROM RETURNING
        cur.execute(
            SQL("""DELETE FROM {}.{} WHERE schema_name = %s AND table_name = %s
                                   RETURNING action, row_data, changed_fields""").format(
                Identifier("audit"), Identifier("logged_actions")), (repository.to_schema(), table))
        # Accumulate the diff in-memory.
        changeset = {}
        for action, row_data, changed_fields in cur:
            conflate_changes(changeset, convert_audit_change(action, row_data, changed_fields, ri_cols))

        if changeset:
            changeset = [tuple(list(pk) + [kind_data[0], json.dumps(kind_data[1])]) for pk, kind_data in
                         changeset.items()]
            query = SQL("INSERT INTO {}.{} ").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                     Identifier(object_id)) + \
                    SQL("VALUES (" + ','.join('%s' for _ in range(len(changeset[0]))) + ")")
            execute_batch(cur, query, changeset, page_size=1000)

            for parent_id, _ in table_info:
                register_object(object_id, object_format='DIFF', namespace=repository.namespace,
                                parent_object=parent_id)

            register_table(repository, table, image_hash, object_id)
        else:
            # Changes in the audit log cancelled each other out. Delete the diff table and just point
            # the commit to the old table objects.
            cur.execute(
                SQL("DROP TABLE {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                               Identifier(object_id)))
            for prev_object_id, _ in table_info:
                register_table(repository, table, image_hash, prev_object_id)


def _convert_vals(vals):
    """Psycopg returns jsonb objects as dicts but doesn't actually accept them directly
    as a query param. Hence, we have to wrap them in the Json datatype when applying a DIFF
    to a table."""
    # This might become a bottleneck since we call this for every row in the diff + function
    # calls are expensive in Python -- maybe there's a better way (e.g. tell psycopg to not convert
    # things to dicts or apply diffs in-driver).
    return [v if not isinstance(v, dict) else Json(v) for v in vals]


def record_table_as_snap(repository, image_hash, table, table_info):
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
