"""
Public API functions to get information about Splitgraph images and repositories.
"""

from splitgraph._data.common import select
from splitgraph._data.images import get_image_object_path
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.engine import get_engine, ResultShape
from splitgraph.exceptions import SplitGraphException


def get_canonical_image_id(repository, short_image):
    """
    Converts a truncated image hash into a full hash. Raises if there's an ambiguity.

    :param repository: Repository the image belongs to
    :param short_image: Shortened image hash
    """
    candidates = get_engine().run_sql(select("images", "image_hash",
                                             "namespace = %s AND repository = %s AND image_hash LIKE %s"),
                                      (repository.namespace, repository.repository, short_image.lower() + '%'),
                                      return_shape=ResultShape.MANY_ONE)

    if not candidates:
        raise SplitGraphException("No snapshots beginning with %s found for mountpoint %s!" % (short_image,
                                                                                               repository.to_schema()))

    if len(candidates) > 1:
        result = "Multiple suitable candidates found: \n * " + "\n * ".join(candidates)
        raise SplitGraphException(result)

    return candidates[0]


def table_schema_changed(repository, table_name, image_1, image_2=None):
    """
    Checks if the table schema has changed between two images in a repository

    :param repository: Repository object
    :param table_name: Table name
    :param image_1: Hash of the first image
    :param image_2: Hash of the second image. If not specified, uses the current staging area.
    """
    snap_1 = get_image_object_path(repository, table_name, image_1)[0]
    # image_2 = None here means the current staging area.
    if image_2 is not None:
        snap_2 = get_image_object_path(repository, table_name, image_2)[0]
        return get_engine().get_full_table_schema(SPLITGRAPH_META_SCHEMA, snap_1) != \
               get_engine().get_full_table_schema(SPLITGRAPH_META_SCHEMA, snap_2)
    return get_engine().get_full_table_schema(SPLITGRAPH_META_SCHEMA, snap_1) != \
           get_engine().get_full_table_schema(repository.to_schema(), table_name)


def get_schema_at(repository, table_name, image_hash):
    """
    Gets the schema of a given table in a given Splitgraph image.

    :param repository: Repository object
    :param table_name: Table name
    :param image_hash: Hash of the image
    :return: The table schema. See the documentation for `get_full_table_schema` for the spec.
    """
    snap_1 = get_image_object_path(repository, table_name, image_hash)[0]
    return get_engine().get_full_table_schema(SPLITGRAPH_META_SCHEMA, snap_1)
