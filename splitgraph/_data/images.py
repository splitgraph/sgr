"""
Internal functions for accessing image metadata
"""
import itertools
from collections import defaultdict
from datetime import datetime

from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier

from splitgraph._data.common import insert
from splitgraph._data.objects import get_full_object_tree, get_object_for_table
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.exceptions import SplitGraphException


def _get_all_child_images(repository, start_image):
    """
    Get all children of `start_image` of any degree
    """

    all_images = repository.get_images()
    result_size = 1
    result = {start_image}
    while True:
        # Keep expanding the set of children until it stops growing
        for image in all_images:
            if image.parent_id in result:
                result.add(image.image_hash)
        if len(result) == result_size:
            return result
        result_size = len(result)


def _get_all_parent_images(repository, start_images):
    """
    Get all parents of the 'start_images' set of any degree.
    Like `_get_all_child_images`, but vice versa.

    Used by the pruning process to identify all images in the same repo
    that are required by images with tags.
    """
    parent = {image.image_hash: image.parent_id for image in repository.get_images()}
    result = set(start_images)
    result_size = len(result)
    while True:
        # Keep expanding the set of parents until it stops growing
        result.update({parent[image] for image in result if parent[image] is not None})
        if len(result) == result_size:
            return result
        result_size = len(result)


def get_image_object_path(repository, table, image):
    """
    Calculates a list of objects SNAP, DIFF, ... , DIFF that are used to reconstruct a table.

    :param repository: Repository the table belongs to
    :param table: Name of the table
    :param image: Image hash the table is stored in.
    :return: A tuple of (SNAP object, list of DIFF objects in reverse order (latest object first))
    """
    path = []
    object_id = get_object_for_table(repository, table, image, object_format='SNAP')
    if object_id is not None:
        return object_id, path

    object_id = get_object_for_table(repository, table, image, object_format='DIFF')

    # Here, we have to follow the object tree up until we encounter a parent of type SNAP -- firing a query
    # for every object is a massive bottleneck.
    # This could be done with a recursive PG query in the future, but currently we just load the whole tree
    # and crawl it in memory.
    object_tree = defaultdict(list)
    for oid, pid, object_format in get_full_object_tree():
        object_tree[oid].append((pid, object_format))

    while object_id is not None:
        path.append(object_id)
        for parent_id, object_format in object_tree[object_id]:
            # Check the _parent_'s format -- if it's a SNAP, we're done
            if object_tree[parent_id][0][1] == 'SNAP':
                return parent_id, path
            object_id = parent_id
            break  # Found 1 diff, will be added to the path at the next iteration.

    # We didn't find an actual snapshot for this table -- something's wrong with the object tree.
    raise SplitGraphException("Couldn't find a SNAP object for %s (malformed object tree)" % table)


def add_new_image(repository, parent_id, image, created=None, comment=None, provenance_type=None, provenance_data=None):
    """
    Registers a new image in the Splitgraph image tree.

    :param repository: Repository the image belongs to
    :param parent_id: Parent of the image
    :param image: Image hash
    :param created: Creation time (defaults to current timestamp)
    :param comment: Comment (defaults to empty)
    :param provenance_type: Image provenance that can be used to rebuild the image
        (one of None, FROM, MOUNT, IMPORT, SQL)
    :param provenance_data: Extra provenance data (dictionary).
    """
    repository.engine.run_sql(
        insert("images", ("image_hash", "namespace", "repository", "parent_id", "created", "comment",
                          "provenance_type", "provenance_data")),
        (image, repository.namespace, repository.repository, parent_id, created or datetime.now(),
         comment, provenance_type, Json(provenance_data)),
        return_shape=None)


def delete_images(repository, images):
    """
    Deletes a set of Splitgraph images from the current engine. Note this doesn't check whether
    this will orphan some other images in the repository.

    :param repository: Repository the images belong to
    :param images: List of image IDs
    """
    if not images:
        return
    # Maybe better to have ON DELETE CASCADE on the FK constraints instead of going through
    # all tables to clean up -- but then we won't get alerted when we accidentally try
    # to delete something that does have FKs relying on it.
    args = tuple([repository.namespace, repository.repository] + list(images))
    for table in ['tags', 'tables', 'images']:
        repository.engine.run_sql(SQL("DELETE FROM {}.{} WHERE namespace = %s AND repository = %s "
                                      "AND image_hash IN (" + ','.join(itertools.repeat('%s', len(images))) + ")")
                                  .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(table)), args,
                                  return_shape=None)
