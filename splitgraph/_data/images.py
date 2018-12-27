"""
Internal functions for accessing image metadata
"""
import itertools
from datetime import datetime

from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core._common import insert


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
