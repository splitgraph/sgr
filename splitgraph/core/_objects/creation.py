"""
Internal functions for packaging changes into physical Splitgraph objects.
"""

from psycopg2.extras import Json

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core._objects.utils import conflate_changes, get_random_object_id


# TODO push these into object_manager


def record_table_as_diff(table, image_hash):
    """
    Flushes the pending changes from the audit table for a given table and records them, registering the new objects.

    :param table: Table object pointing to the HEAD table
    :param image_hash: Image hash to store the table under
    """
    object_id = get_random_object_id()
    engine = table.repository.engine
    # Accumulate the diff in-memory. This might become a bottleneck in the future.
    changeset = {}
    for action, row_data, changed_fields in engine.get_pending_changes(table.repository.to_schema(), table.table_name):
        conflate_changes(changeset, [(action, row_data, changed_fields)])
    engine.discard_pending_changes(table.repository.to_schema(), table.table_name)
    changeset = [tuple(list(pk) + [kind_data[0], Json(kind_data[1])]) for pk, kind_data in
                 changeset.items()]

    if changeset:
        engine.store_diff_object(changeset, SPLITGRAPH_META_SCHEMA, object_id,
                                 change_key=engine.get_change_key(table.repository.to_schema(), table.table_name))
        for parent_id, _ in table.objects:
            table.repository.objects.register_object(
                object_id, object_format='DIFF', namespace=table.repository.namespace, parent_object=parent_id)

        table.repository.objects.register_table(table.repository, table.table_name, image_hash, object_id)
    else:
        # Changes in the audit log cancelled each other out. Delete the diff table and just point
        # the commit to the old table objects.
        for prev_object_id, _ in table.objects:
            table.repository.objects.register_table(table.repository, table.table_name, image_hash, prev_object_id)


def record_table_as_snap(table, image_hash, repository=None, table_name=None):
    """
    Copies the full table verbatim into a new Splitgraph SNAP object, registering the new object.

    :param image_hash: Hash of the new image
    :param table: Table object
    """

    table_name = table_name or table.table_name
    repository = repository or table.repository
    
    # Make sure we didn't actually create a snap for this table.
    if repository.get_image(image_hash).get_table(table_name) is None \
            or repository.get_image(image_hash).get_table(table_name).get_object('SNAP') is None:
        object_id = get_random_object_id()
        repository.engine.copy_table(repository.to_schema(), table_name, SPLITGRAPH_META_SCHEMA, object_id,
                                     with_pk_constraints=True)
        if table and table.objects:
            for parent_id, _ in table.objects:
                repository.objects.register_object(object_id, object_format='SNAP', namespace=repository.namespace,
                                                   parent_object=parent_id)
        else:
            repository.objects.register_object(object_id, object_format='SNAP', namespace=repository.namespace,
                                               parent_object=None)
        repository.objects.register_table(repository, table_name, image_hash, object_id)
