import logging

from splitgraph.config import SPLITGRAPH_META_SCHEMA


class Table:
    """Represents a Splitgraph table in a given image. Shouldn't be created directly, use Table-loading
    methods in the :class:`splitgraph.core.Image` class instead."""
    def __init__(self, repository, image, table_name, objects):
        self.repository = repository
        self.image = image
        self.table_name = table_name
        self.objects = objects

    def materialize(self, destination, destination_schema=None):
        """
        Materializes a Splitgraph table in the target schema as a normal Postgres table, potentially downloading all
        required objects and using them to reconstruct the table.

        :param destination: Name of the destination table.
        :param destination_schema: Name of the destination schema.
        :return: A set of IDs of downloaded objects used to construct the table.
        """
        destination_schema = destination_schema or self.repository.to_schema()
        engine = self.repository.engine
        object_manager = self.repository.objects
        engine.delete_table(destination_schema, destination)
        # Get the closest snapshot from the table's parents
        # and then apply all deltas consecutively from it.
        object_id, to_apply = object_manager.get_image_object_path(self)

        # Make sure all the objects have been downloaded from remote if it exists
        upstream = self.repository.get_upstream()
        if upstream:
            object_locations = object_manager.get_external_object_locations(to_apply + [object_id])
            fetched_objects = object_manager.download_objects(upstream.objects,
                                                              objects_to_fetch=to_apply + [object_id],
                                                              object_locations=object_locations)

        difference = set(to_apply + [object_id]).difference(set(object_manager.get_existing_objects()))
        if difference:
            logging.warning("Not all objects required to materialize %s:%s:%s exist locally.",
                            destination_schema, self.image.image_hash, self.table_name)
            logging.warning("Missing objects: %r", difference)

        # Copy the given snap id over to "staging" and apply the DIFFS
        engine.copy_table(SPLITGRAPH_META_SCHEMA, object_id, destination_schema, destination,
                          with_pk_constraints=True)
        for pack_object in reversed(to_apply):
            logging.info("Applying %s...", pack_object)
            engine.apply_diff_object(SPLITGRAPH_META_SCHEMA, pack_object, destination_schema, destination)

        return fetched_objects if upstream else set()

    def get_schema(self):
        """
        Gets the schema of a given table

        :return: The table schema. See the documentation for `get_full_table_schema` for the spec.
        """
        snap_1 = self.repository.objects.get_image_object_path(self)[0]
        return self.repository.engine.get_full_table_schema(SPLITGRAPH_META_SCHEMA, snap_1)

    def get_object(self, object_type):
        """
        Get the physical object ID of a given type that this table is linked to

        :param object_type: Either SNAP or DIFF
        :return: Object ID or None if an object of such type doesn't exist
        """

        for object_id, _object_type in self.objects:
            if _object_type == object_type:
                return object_id
        return None
