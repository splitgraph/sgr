import splitgraph as sg
from splitgraph.core.table import Table


class Image(sg.commands.info.Image):
    """
    Shim, experimenting with sg OO API. Inherits from the Image namedtuple for now
    """
    def get_tables(self):
        return sg.get_tables_at(repository=self.repository.repository, image=self.image_hash)

    def get_table(self, table_name):
        objects = sg.get_table(self.repository.repository, table_name, self.image_hash)
        return Table(self.repository, self, table_name, objects)

    def provenance(self):
        return sg.provenance(self.repository.repository, self.image_hash)

    def to_splitfile(self):
        return sg.image_hash_to_splitfile(self.repository.repository, self.image_hash)

    def tag(self, tag, force=False):
        sg.set_tag(self.repository.repository, self.image_hash, tag, force)

    def get_tags(self):
        return [t for h, t in sg.get_all_hashes_tags(self.repository.repository) if h == self.image_hash]

    # todo image deletion -- here or in the Repository?
