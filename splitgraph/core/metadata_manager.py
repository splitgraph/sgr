"""
Classes related to managing table/image/object metadata tables.
"""
import itertools
from collections import namedtuple

from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_API_SCHEMA
from ._common import select, ResultShape

OBJECT_COLS = ['object_id', 'format', 'parent_id', 'namespace', 'size', 'insertion_hash', 'deletion_hash', 'index']


class Object(namedtuple('Object', OBJECT_COLS)):
    """Represents a Splitgraph object that tables are composed of."""


class MetadataManager:
    """
    A data access layer for the metadata tables in the splitgraph_meta schema that concerns itself
    with image, table and object information.
    """

    def __init__(self, object_engine, metadata_engine=None):
        self.object_engine = object_engine
        self.metadata_engine = metadata_engine or object_engine

    def register_objects(self, objects, namespace=None):
        """
        Registers multiple Splitgraph objects in the tree.

        :param objects: List of `Object` objects.
        :param namespace: If specified, overrides the original object namespace, required
            in the case where the remote repository has a different namespace than the local one.
        """
        object_meta = [tuple(namespace if namespace and a == 'namespace'
                             else getattr(o, a) for a in OBJECT_COLS) for o in objects]

        self.metadata_engine.run_sql_batch(
            SQL("SELECT {}.add_object(" + ','.join(itertools.repeat('%s', len(OBJECT_COLS))) + ")")
                .format(Identifier(SPLITGRAPH_API_SCHEMA)), object_meta)

    def register_tables(self, repository, table_meta):
        """
        Links tables in an image to physical objects that they are stored as.
        Objects must already be registered in the object tree.

        :param repository: Repository that the tables belong to.
        :param table_meta: A list of (image_hash, table_name, table_schema, object_ids).
        """
        table_meta = [(repository.namespace, repository.repository,
                       o[0], o[1], Json(o[2]), o[3]) for o in table_meta]
        self.metadata_engine.run_sql_batch(
            SQL("SELECT {}.add_table(%s,%s,%s,%s,%s,%s)").format(Identifier(SPLITGRAPH_API_SCHEMA)),
            table_meta)

    def register_object_locations(self, object_locations):
        """
        Registers external locations (e.g. HTTP or S3) for Splitgraph objects.
        Objects must already be registered in the object tree.

        :param object_locations: List of (object_id, location, protocol).
        """
        self.metadata_engine.run_sql_batch(
            SQL("SELECT {}.add_object_location(%s,%s,%s)").format(Identifier(SPLITGRAPH_API_SCHEMA)),
            object_locations)

    def get_all_objects(self):
        """
        Gets all objects currently in the Splitgraph tree.

        :return: Set of object IDs.
        """
        return set(self.metadata_engine.run_sql(select("objects", "object_id"), return_shape=ResultShape.MANY_ONE))

    def get_new_objects(self, object_ids):
        """
        Get object IDs from the passed list that don't exist in the tree.

        :param object_ids: List of objects to check
        :return: List of unknown object IDs.
        """
        return self.metadata_engine.run_sql(
            SQL("SELECT {}.get_new_objects(%s)").format(Identifier(SPLITGRAPH_API_SCHEMA)),
            (object_ids,), return_shape=ResultShape.ONE_ONE)

    def get_external_object_locations(self, objects):
        """
        Gets external locations for objects.

        :param objects: List of object IDs stored externally.
        :return: List of (object_id, location, protocol).
        """
        return self.metadata_engine.run_sql(select("get_object_locations", "object_id, location, protocol",
                                                   schema=SPLITGRAPH_API_SCHEMA, table_args="(%s)"), (objects,))

    def get_object_meta(self, objects):
        """
        Get metadata for multiple Splitgraph objects from the tree

        :param objects: List of objects to get metadata for.
        :return: Dictionary of object_id -> Object
        """
        if not objects:
            return {}

        metadata = self.metadata_engine.run_sql(select("get_object_meta", ','.join(OBJECT_COLS),
                                                       schema=SPLITGRAPH_API_SCHEMA, table_args="(%s)"), (objects,))
        result = [Object(*m) for m in metadata]
        return {o.object_id: o for o in result}
