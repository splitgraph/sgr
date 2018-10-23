import logging
from contextlib import contextmanager

from psycopg2.sql import Identifier, SQL

from splitgraph.commands.misc import delete_objects
from splitgraph.constants import get_random_object_id, SplitGraphException, SPLITGRAPH_META_SCHEMA
from splitgraph.meta_handler.images import get_canonical_image_id, get_closest_parent_image_object
from splitgraph.meta_handler.misc import get_remote_for
from splitgraph.meta_handler.objects import get_external_object_locations
from splitgraph.meta_handler.tables import get_tables_at, get_object_for_table
from splitgraph.meta_handler.tags import get_tagged_id, set_head
from splitgraph.objects.applying import apply_record_to_staging
from splitgraph.objects.loading import download_objects
from splitgraph.pg_audit import has_pending_changes, manage_audit, discard_pending_changes
from splitgraph.pg_utils import copy_table, get_primary_keys, get_all_tables


def materialize_table(conn, repository, image_hash, table, destination, destination_schema=None):
    """
    Materializes a SplitGraph table in the target schema as a normal Postgres table, potentially downloading all
    required objects and using them to reconstruct the table.

    :param conn: psycopg connection object
    :param repository: Target mountpoint to materialize the table in.
    :param image_hash: Hash of the commit to get the table from.
    :param table: Name of the table.
    :param destination: Name of the destination table.
    :param destination_schema: Name of the destination schema.
    :return: A set of IDs of downloaded objects used to construct the table.
    """
    destination_schema = destination_schema or repository.to_schema()
    with conn.cursor() as cur:
        cur.execute(
            SQL("DROP TABLE IF EXISTS {}.{}").format(Identifier(destination_schema), Identifier(destination)))
        # Get the closest snapshot from the table's parents
        # and then apply all deltas consecutively from it.
        object_id, to_apply = get_closest_parent_image_object(conn, repository, table, image_hash)

        # Make sure all the objects have been downloaded from remote if it exists
        remote_info = get_remote_for(conn, repository)
        if remote_info:
            remote_conn, _ = remote_info
            object_locations = get_external_object_locations(conn, to_apply + [object_id])
            fetched_objects = download_objects(conn, remote_conn, objects_to_fetch=to_apply + [object_id],
                                               object_locations=object_locations)

        # Copy the given snap id over to "staging"
        copy_table(conn, SPLITGRAPH_META_SCHEMA, object_id, destination_schema, destination,
                   with_pk_constraints=True)
        # This is to work around logical replication not reflecting deletions from non-PKd tables. However, this makes
        # it emit all column values in the row, not just the updated ones.
        if not get_primary_keys(conn, destination_schema, destination):
            logging.warning("Table %s has no primary key. "
                            "This means that changes will have to be recorded as whole-row.", destination)
            cur.execute(SQL("ALTER TABLE {}.{} REPLICA IDENTITY FULL").format(Identifier(destination_schema),
                                                                              Identifier(destination)))
        else:
            cur.execute(SQL("ALTER TABLE {}.{} REPLICA IDENTITY DEFAULT").format(Identifier(destination_schema),
                                                                                 Identifier(destination)))

        # Apply the deltas sequentially to the checked out table
        for pack_object in reversed(to_apply):
            logging.info("Applying %s...", pack_object)
            apply_record_to_staging(conn, pack_object, destination_schema, destination)

        return fetched_objects if remote_info else set()


@manage_audit
def checkout(conn, repository, image_hash=None, tag=None, tables=None, keep_downloaded_objects=True):
    """
    Discards all pending changes in the current mountpoint and checks out an image, changing the current HEAD pointer.

    :param conn: psycopg connection object
    :param repository: Mountpoint to check out.
    :param image_hash: Hash of the image to check out.
    :param tag: Tag of the image to check out. One of `image_hash` or `tag` must be specified.
    :param tables: List of tables to materialize in the mountpoint.
    :param keep_downloaded_objects: If False, deletes externally downloaded objects after they've been used.
    :param namespace: Namespace
    """
    target_schema = repository.to_schema()
    if tables is None:
        tables = []
    if has_pending_changes(conn, repository):
        logging.warning("%s has pending changes, discarding...", repository)
    discard_pending_changes(conn, target_schema)
    # Detect the actual schema snap we want to check out
    if image_hash:
        # get_canonical_image_hash called twice if the commandline entry point already called it. How to fix?
        image_hash = get_canonical_image_id(conn, repository, image_hash)
    elif tag:
        image_hash = get_tagged_id(conn, repository, tag)
    else:
        raise SplitGraphException("One of schema_snap or tag must be specified!")

    tables = tables or get_tables_at(conn, repository, image_hash)
    with conn.cursor() as cur:
        # Drop all current tables in staging
        for table in get_all_tables(conn, target_schema):
            cur.execute(SQL("DROP TABLE IF EXISTS {}.{}").format(Identifier(target_schema), Identifier(table)))

    downloaded_object_ids = set()
    for table in tables:
        downloaded_object_ids |= materialize_table(conn, repository, image_hash, table, table)

    # Repoint the current HEAD for this mountpoint to the new snap ID
    set_head(conn, repository, image_hash)

    if not keep_downloaded_objects:
        logging.info("Removing %d downloaded objects from cache..." % len(downloaded_object_ids))
        delete_objects(conn, downloaded_object_ids)


@contextmanager
def materialized_table(conn, repository, table_name, image_hash):
    """A context manager that returns a pointer to a read-only materialized table in a given image.
    If the table is already stored as a SNAP, this doesn't use any extra space.
    Otherwise, the table is materialized and deleted on exit from the context manager.

    :param conn: Psycopg connection object
    :param repository: Repository that the table belongs to
    :param table_name: Name of the table
    :param image_hash: Image hash to materialize
    :return: (schema, table_name) where the materialized table is located.
        The table must not be changed, as it might be a pointer to a real SG SNAP object.
    """
    if image_hash is None:
        # No snapshot -- just return the current staging table.
        yield repository.to_schema(), table_name
    # See if the table snapshot already exists, otherwise reconstruct it
    object_id = get_object_for_table(conn, repository, table_name, image_hash, 'SNAP')
    if object_id is None:
        # Materialize the SNAP into a new object
        new_id = get_random_object_id()
        materialize_table(conn, repository, image_hash, table_name, new_id, destination_schema=SPLITGRAPH_META_SCHEMA)
        yield SPLITGRAPH_META_SCHEMA, new_id
        # Maybe some cache management/expiry strategies here
        delete_objects(conn, [new_id])
    else:
        yield SPLITGRAPH_META_SCHEMA, object_id
