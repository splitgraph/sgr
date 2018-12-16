"""
Internal functions for packaging changes into physical Splitgraph objects.
"""

from psycopg2.extras import Json

from splitgraph._data.objects import register_table, register_object, get_object_for_table
from splitgraph.commands._objects.utils import conflate_changes, get_random_object_id
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.engine import get_engine


def record_table_as_diff(repository, image_hash, table, table_info):
    """
    Flushes the pending changes from the audit table for a given table and records them, registering the new objects.
    :param repository: Repository object
    :param image_hash: Hash of the new image
    :param table: Table name
    :param table_info: Information for the previous version of this table (list of object IDs and their formats)
    """
    object_id = get_random_object_id()
    changeset = _get_conflated_changeset(repository, table)

    if changeset:
        engine = get_engine()
        engine.store_diff_object(changeset, SPLITGRAPH_META_SCHEMA, object_id,
                                 change_key=engine.get_change_key(repository.to_schema(), table))
        for parent_id, _ in table_info:
            register_object(object_id, object_format='DIFF', namespace=repository.namespace,
                            parent_object=parent_id)

        register_table(repository, table, image_hash, object_id)
    else:
        # Changes in the audit log cancelled each other out. Delete the diff table and just point
        # the commit to the old table objects.
        for prev_object_id, _ in table_info:
            register_table(repository, table, image_hash, prev_object_id)


def _get_conflated_changeset(repository, table):
    # Accumulate the diff in-memory. This might become a bottleneck in the future.
    changeset = {}
    engine = get_engine()
    for action, row_data, changed_fields in engine.get_pending_changes(repository.to_schema(), table):
        conflate_changes(changeset, [(action, row_data, changed_fields)])
    engine.discard_pending_changes(repository.to_schema(), table)
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
        get_engine().copy_table(repository.to_schema(), table, SPLITGRAPH_META_SCHEMA, object_id,
                                with_pk_constraints=True)
        if table_info:
            for parent_id, _ in table_info:
                register_object(object_id, object_format='SNAP', namespace=repository.namespace,
                                parent_object=parent_id)
        else:
            register_object(object_id, object_format='SNAP', namespace=repository.namespace, parent_object=None)
        register_table(repository, table, image_hash, object_id)
