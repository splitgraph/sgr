from collections import namedtuple

from psycopg2.sql import SQL, Identifier

import splitgraph as sg
from splitgraph import get_engine
from splitgraph._data.common import select
from splitgraph._data.images import IMAGE_COLS, get_image_object_path
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core.table import Table
from splitgraph.engine import ResultShape


class Image(namedtuple('Image', IMAGE_COLS)):
    """
    Shim, experimenting with sg OO API.
    """

    def __new__(cls, *args, **kwargs):
        repository = kwargs.pop('repository')
        self = super(Image, cls).__new__(cls, *args, **kwargs)
        self.repository = repository
        return self

    def get_parent_children(self):
        """Gets the parent and a list of children of a given image."""
        parent = self.parent_id

        children = get_engine().run_sql(SQL("""SELECT image_hash FROM {}.images
                WHERE namespace = %s AND repository = %s AND parent_id = %s""")
                                        .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                                        (self.repository.namespace, self.repository.repository, self.image_hash),
                                        return_shape=ResultShape.MANY_ONE)
        return parent, children

    def get_tables(self):
        """
        Gets the names of all tables inside of an image.
        """
        return get_engine().run_sql(
            select('tables', 'table_name', 'namespace = %s AND repository = %s AND image_hash = %s'),
            (self.repository.namespace, self.repository.repository, self.image_hash),
            return_shape=ResultShape.MANY_ONE)

    def get_table(self, table_name):
        """
        Returns a Table object representing a version of a given table.
        Contains a list of objects that the table is linked to: a DIFF object (beginning a chain of DIFFs that
        describe a table), a SNAP object (a full table copy), or both.

        :param table_name: Name of the table
        :return: Table object or None
        """
        objects = get_engine().run_sql(SQL("""SELECT {0}.tables.object_id, format FROM {0}.tables JOIN {0}.objects
                                              ON {0}.objects.object_id = {0}.tables.object_id
                                              WHERE {0}.tables.namespace = %s AND repository = %s AND image_hash = %s
                                              AND table_name = %s""")
                                       .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                                       (self.repository.namespace, self.repository.repository,
                                        self.image_hash, table_name))
        if not objects:
            return None
        return Table(self.repository, self, table_name, objects)

    def provenance(self):
        return sg.provenance(self.repository, self.image_hash)

    def tag(self, tag, force=False):
        sg.set_tag(self.repository, self.image_hash, tag, force)

    def get_tags(self):
        return [t for h, t in sg.get_all_hashes_tags(self.repository) if h == self.image_hash]

    def delete_tag(self, tag):
        sg.delete_tag(self.repository, tag)

    def get_log(self):
        return sg.get_log(self.repository, self.image_hash)

    def to_splitfile(self, err_on_end=True, source_replacement=None):
        return sg.image_hash_to_splitfile(self.repository, self.image_hash, err_on_end, source_replacement)

    # todo image deletion -- here or in the Repository?

    def get_table_schema(self, table):
        """
        Gets the schema of a given table

        :param table: Table name
        :return: The table schema. See the documentation for `get_full_table_schema` for the spec.
        """
        snap_1 = get_image_object_path(self.repository, table, self.image_hash)[0]
        return get_engine().get_full_table_schema(SPLITGRAPH_META_SCHEMA, snap_1)
