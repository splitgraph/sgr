"""
Commands for checking out Splitgraph images
"""

import logging
from contextlib import contextmanager

from splitgraph.commands._common import manage_audit
from splitgraph.commands.repository import get_upstream
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.engine import get_engine
from ._common import set_head
from ._objects.applying import apply_record_to_staging
from ._objects.loading import download_objects
from ._objects.utils import get_random_object_id
from .diff import has_pending_changes
from .info import get_canonical_image_id, get_tables_at
from .misc import delete_objects, rm
from .tagging import get_tagged_id, delete_tag
from .._data.images import get_image_object_path
from .._data.objects import get_external_object_locations, get_object_for_table, get_existing_objects
from ..connection import get_remote_connection_params
from ..exceptions import SplitGraphException


def materialize_table(repository, image_hash, table, destination, destination_schema=None):
    """
    Materializes a Splitgraph table in the target schema as a normal Postgres table, potentially downloading all
    required objects and using them to reconstruct the table.

    :param repository: Target mountpoint to materialize the table in.
    :param image_hash: Hash of the commit to get the table from.
    :param table: Name of the table.
    :param destination: Name of the destination table.
    :param destination_schema: Name of the destination schema.
    :return: A set of IDs of downloaded objects used to construct the table.
    """
    destination_schema = destination_schema or repository.to_schema()
    engine = get_engine()
    engine.delete_table(destination_schema, destination)
    # Get the closest snapshot from the table's parents
    # and then apply all deltas consecutively from it.
    object_id, to_apply = get_image_object_path(repository, table, image_hash)

    # Make sure all the objects have been downloaded from remote if it exists
    remote_info = get_upstream(repository)
    if remote_info:
        object_locations = get_external_object_locations(to_apply + [object_id])
        fetched_objects = download_objects(get_remote_connection_params(remote_info[0]),
                                           objects_to_fetch=to_apply + [object_id],
                                           object_locations=object_locations)

    difference = set(to_apply + [object_id]).difference(set(get_existing_objects()))
    if difference:
        logging.warning("Not all objects required to materialize %s:%s:%s exist locally.",
                        repository.to_schema(), image_hash, table)
        logging.warning("Missing objects: %r", difference)

    # Copy the given snap id over to "staging" and apply the DIFFS
    engine.copy_table(SPLITGRAPH_META_SCHEMA, object_id, destination_schema, destination,
                      with_pk_constraints=True)
    for pack_object in reversed(to_apply):
        logging.info("Applying %s...", pack_object)
        apply_record_to_staging(pack_object, destination_schema, destination)

    return fetched_objects if remote_info else set()


@manage_audit
def checkout(repository, image_hash=None, tag=None, tables=None, keep_downloaded_objects=True, force=False):
    """
    Checks out an image belonging to a given repository, changing the current HEAD pointer. Raises an error
    if there are pending changes to the

    :param repository: Repository to check out.
    :param image_hash: Hash of the image to check out.
    :param tag: Tag of the image to check out. One of `image_hash` or `tag` must be specified.
    :param tables: List of tables to materialize in the mountpoint.
    :param keep_downloaded_objects: If False, deletes externally downloaded objects after they've been used.
    :param force: Discards all pending changes to the schema.
    """
    target_schema = repository.to_schema()
    engine = get_engine()
    if tables is None:
        tables = []
    if has_pending_changes(repository):
        if not force:
            raise SplitGraphException("{0} has pending changes! Pass force=True or do sgr checkout -f {0}:HEAD"
                                      .format(repository.to_schema()))
        logging.warning("%s has pending changes, discarding...", repository.to_schema())
        engine.discard_pending_changes(target_schema)
    # Detect the actual image
    if image_hash:
        # get_canonical_image_hash called twice if the commandline entry point already called it. How to fix?
        image_hash = get_canonical_image_id(repository, image_hash)
    elif tag:
        image_hash = get_tagged_id(repository, tag)
    else:
        raise SplitGraphException("One of image_hash or tag must be specified!")

    tables = tables or get_tables_at(repository, image_hash)
    # Drop all current tables in staging
    for table in engine.get_all_tables(target_schema):
        engine.delete_table(target_schema, table)

    downloaded_object_ids = set()
    for table in tables:
        downloaded_object_ids |= materialize_table(repository, image_hash, table, table)

    # Repoint the current HEAD for this mountpoint to the new snap ID
    set_head(repository, image_hash)

    if not keep_downloaded_objects:
        logging.info("Removing %d downloaded objects from cache...", downloaded_object_ids)
        delete_objects(downloaded_object_ids)


@manage_audit
def uncheckout(repository, force=False):
    """
    Deletes the schema that the repository is checked out into

    :param repository: Repository to check out.
    :param force: Discards all pending changes to the schema.
    """
    if has_pending_changes(repository):
        if not force:
            raise SplitGraphException("{0} has pending changes! Pass force=True or do sgr checkout -f {0}:HEAD"
                                      .format(repository.to_schema()))
        logging.warning("%s has pending changes, discarding...", repository.to_schema())

    # Delete the schema and remove the HEAD tag
    rm(repository, unregister=False)
    delete_tag(repository, 'HEAD')


@contextmanager
def materialized_table(repository, table_name, image_hash):
    """A context manager that returns a pointer to a read-only materialized table in a given image.
    If the table is already stored as a SNAP, this doesn't use any extra space.
    Otherwise, the table is materialized and deleted on exit from the context manager.

    :param repository: Repository that the table belongs to
    :param table_name: Name of the table
    :param image_hash: Image hash to materialize
    :return: (schema, table_name) where the materialized table is located.
        The table must not be changed, as it might be a pointer to a real SG SNAP object.
    """
    if image_hash is None:
        # No image hash -- just return the current staging table.
        yield repository.to_schema(), table_name
    # See if the table snapshot already exists, otherwise reconstruct it
    object_id = get_object_for_table(repository, table_name, image_hash, 'SNAP')
    if object_id is None:
        # Materialize the SNAP into a new object
        new_id = get_random_object_id()
        materialize_table(repository, image_hash, table_name, new_id, destination_schema=SPLITGRAPH_META_SCHEMA)
        yield SPLITGRAPH_META_SCHEMA, new_id
        # Maybe some cache management/expiry strategies here
        delete_objects([new_id])
    else:
        if get_engine().table_exists(SPLITGRAPH_META_SCHEMA, object_id):
            yield SPLITGRAPH_META_SCHEMA, object_id
        else:
            # The SNAP object doesn't actually exist remotely, so we have to download it.
            # An optimisation here: we could open an RO connection to the remote instead if the object
            # does live there.
            remote_info = get_upstream(repository)
            if not remote_info:
                raise SplitGraphException("SNAP %s from %s doesn't exist locally and no remote was found for it!"
                                          % (object_id, str(repository)))
            remote_conn, _ = remote_info
            object_locations = get_external_object_locations([object_id])
            download_objects(remote_conn, objects_to_fetch=[object_id], object_locations=object_locations)
            yield SPLITGRAPH_META_SCHEMA, object_id

            delete_objects([object_id])
