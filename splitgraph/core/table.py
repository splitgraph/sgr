"""Table metadata-related classes."""

import logging

from psycopg2.sql import SQL, Identifier
from splitgraph.config import SPLITGRAPH_META_SCHEMA


class Table:
    """Represents a Splitgraph table in a given image. Shouldn't be created directly, use Table-loading
    methods in the :class:`splitgraph.core.image.Image` class instead."""

    def __init__(self, repository, image, table_name, table_schema, objects):
        self.repository = repository
        self.image = image
        self.table_name = table_name
        self.table_schema = [tuple(entry) for entry in table_schema]

        # List of fragments this table is composed of
        self.objects = objects

    def materialize(self, destination, destination_schema=None, lq_server=None):
        """
        Materializes a Splitgraph table in the target schema as a normal Postgres table, potentially downloading all
        required objects and using them to reconstruct the table.

        :param destination: Name of the destination table.
        :param destination_schema: Name of the destination schema.
        :param lq_server: If set, sets up a layered querying FDW for the table instead using this foreign server.
        """
        destination_schema = destination_schema or self.repository.to_schema()
        engine = self.repository.engine
        object_manager = self.repository.objects
        engine.delete_table(destination_schema, destination)

        if not lq_server:
            # Copy the given snap id over to "staging" and apply the DIFFS
            with object_manager.ensure_objects(self) as required_objects:
                engine.copy_table(SPLITGRAPH_META_SCHEMA, required_objects[0], destination_schema, destination,
                                  with_pk_constraints=True)
                if len(required_objects) > 1:
                    logging.info("Applying %d fragment(s)...", (len(required_objects) - 1))
                    engine.apply_fragments([(SPLITGRAPH_META_SCHEMA, d) for d in required_objects[1:]],
                                           destination_schema, destination)
        else:
            query = SQL("CREATE FOREIGN TABLE {}.{} (") \
                .format(Identifier(destination_schema), Identifier(self.table_name))
            query += SQL(','.join(
                "{} %s " % ctype for _, _, ctype, _ in self.table_schema)).format(
                *(Identifier(cname) for _, cname, _, _ in self.table_schema))
            query += SQL(") SERVER {} OPTIONS (table %s)").format(Identifier(lq_server))
            engine.run_sql(query, (self.table_name,))
