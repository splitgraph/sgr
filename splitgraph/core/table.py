import logging

from splitgraph import get_engine
from splitgraph._data.images import get_image_object_path
from splitgraph._data.objects import get_external_object_locations, get_existing_objects
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core._objects.loading import download_objects


class Table:
    def __init__(self, repository, image, table_name, objects):
        self.repository = repository
        self.image = image
        self.table_name = table_name
        self.objects = objects

    # todo object API too

    def materialize(self, destination, destination_schema=None):
        """
        Materializes a Splitgraph table in the target schema as a normal Postgres table, potentially downloading all
        required objects and using them to reconstruct the table.

        :param destination: Name of the destination table.
        :param destination_schema: Name of the destination schema.
        :return: A set of IDs of downloaded objects used to construct the table.
        """
        destination_schema = destination_schema or self.repository.to_schema()
        engine = get_engine()
        engine.delete_table(destination_schema, destination)
        # Get the closest snapshot from the table's parents
        # and then apply all deltas consecutively from it.
        object_id, to_apply = get_image_object_path(self.repository, self.table_name, self.image.image_hash)

        # Make sure all the objects have been downloaded from remote if it exists
        remote_info = self.repository.get_upstream()
        if remote_info:
            object_locations = get_external_object_locations(to_apply + [object_id])
            fetched_objects = download_objects(remote_info[0],
                                               objects_to_fetch=to_apply + [object_id],
                                               object_locations=object_locations)

        difference = set(to_apply + [object_id]).difference(set(get_existing_objects()))
        if difference:
            logging.warning("Not all objects required to materialize %s:%s:%s exist locally.",
                            destination_schema(), self.image.image_hash, self.table_name)
            logging.warning("Missing objects: %r", difference)

        # Copy the given snap id over to "staging" and apply the DIFFS
        engine.copy_table(SPLITGRAPH_META_SCHEMA, object_id, destination_schema, destination,
                          with_pk_constraints=True)
        for pack_object in reversed(to_apply):
            logging.info("Applying %s...", pack_object)
            engine.apply_diff_object(SPLITGRAPH_META_SCHEMA, pack_object, destination_schema, destination)

        return fetched_objects if remote_info else set()
