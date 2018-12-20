from collections import namedtuple

import splitgraph as sg
from splitgraph._data.images import IMAGE_COLS
from splitgraph.core.table import Table


class Image(namedtuple('Image', IMAGE_COLS)):
    """
    Shim, experimenting with sg OO API. Inherits from the Image namedtuple for now
    """

    def __new__(cls, *args, **kwargs):
        repository = kwargs.pop('repository')
        self = super(Image, cls).__new__(cls, *args, **kwargs)
        self.repository = repository
        return self

    def get_tables(self):
        return sg.get_tables_at(repository=self.repository, image=self.image_hash)

    def get_table(self, table_name):
        objects = sg.get_table(self.repository, table_name, self.image_hash)
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
