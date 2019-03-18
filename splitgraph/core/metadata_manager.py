"""
Classes related to managing table/image/object metadata tables.
"""

from psycopg2.extras import Json

from ._common import insert, select, ResultShape


class MetadataManager:
    """
    A data access layer for the metadata tables in the splitgraph_meta schema that concerns itself
    with image, table and object information.
    """

    def __init__(self, object_engine):
        self.object_engine = object_engine

    def register_objects(self, object_meta, namespace=None):
        """
        Registers multiple Splitgraph objects in the tree. See `register_object` for more information.

        :param object_meta: List of (object_id, format, parent_id, namespace, size, index).
        :param namespace: If specified, overrides the original object namespace, required
            in the case where the remote repository has a different namespace than the local one.
        """
        if namespace:
            object_meta = [(o, f, p, namespace, s, i) for o, f, p, _, s, i in object_meta]
        self.object_engine.run_sql_batch(
            insert("objects", ("object_id", "format", "parent_id", "namespace", "size", "index")),
            object_meta)

    def register_tables(self, repository, table_meta):
        """
        Links tables in an image to physical objects that they are stored as.
        Objects must already be registered in the object tree.

        :param repository: Repository that the tables belong to.
        :param table_meta: A list of (image_hash, table_name, table_schema, object_ids).
        """
        table_meta = [(repository.namespace, repository.repository,
                       o[0], o[1], Json(o[2]), o[3]) for o in table_meta]
        self.object_engine.run_sql_batch(
            insert("tables", ("namespace", "repository", "image_hash", "table_name", "table_schema", "object_ids")),
            table_meta)

    def register_object_locations(self, object_locations):
        """
        Registers external locations (e.g. HTTP or S3) for Splitgraph objects.
        Objects must already be registered in the object tree.

        :param object_locations: List of (object_id, location, protocol).
        """
        # Don't insert redundant objects here either.
        existing_locations = self.object_engine.run_sql(select("object_locations", "object_id"),
                                                        return_shape=ResultShape.MANY_ONE)
        object_locations = [o for o in object_locations if o[0] not in existing_locations]
        self.object_engine.run_sql_batch(insert("object_locations", ("object_id", "location", "protocol")),
                                         object_locations)

    def get_existing_objects(self):
        """
        Gets all objects currently in the Splitgraph tree.

        :return: Set of object IDs.
        """
        return set(self.object_engine.run_sql(select("objects", "object_id"), return_shape=ResultShape.MANY_ONE))

    def get_external_object_locations(self, objects):
        """
        Gets external locations for objects.

        :param objects: List of objects stored externally.
        :return: List of (object_id, location, protocol).
        """
        return self.object_engine.run_sql(select("object_locations", "object_id, location, protocol",
                                                 "object_id IN (" + ','.join('%s' for _ in objects) + ")"),
                                          objects)

    def get_object_meta(self, objects):
        """
        Get metadata for multiple Splitgraph objects from the tree

        :param objects: List of objects to get metadata for.
        :return: List of (object_id, format, parent_id, namespace, size, index).
        """
        return self.object_engine.run_sql(select("objects", "object_id, format, parent_id, namespace, size, index",
                                                 "object_id IN (" + ','.join('%s' for _ in objects) + ")"), objects)

    def extract_recursive_object_meta(self, remote, table_meta):
        """Recursively crawl the a remote object manager in order to fetch all objects
        required to materialize tables specified in `table_meta` that don't yet exist on the local engine."""
        existing_objects = self.get_existing_objects()
        distinct_objects = set(o for os in table_meta for o in os[3] if o not in existing_objects)
        known_objects = set()
        object_meta = []

        while True:
            new_parents = [o for o in distinct_objects if o not in known_objects]
            if not new_parents:
                break
            else:
                parents_meta = remote.get_object_meta(new_parents)
                distinct_objects.update(
                    set(o for os in parents_meta for o in os[3] if o not in existing_objects))
                object_meta.extend(parents_meta)
                known_objects.update(new_parents)
        return distinct_objects, object_meta
