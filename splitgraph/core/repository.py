import splitgraph as sg
from splitgraph.commands.diff import has_pending_changes as _has_changes
from splitgraph.commands.repository import get_upstream, set_upstream
from .image import Image


class Repository:
    """
    Shim, experimenting with Splitgraph OO API

    Currently calls into the real API functions but we'll obviously move them here if we go ahead with this.
    """

    def __init__(self, namespace, repository, engine=None):
        self.namespace = namespace
        self.repository = repository

        self.engine = engine or sg.get_engine()

    def __eq__(self, other):
        return self.namespace == other.namespace and self.repository == other.repository and self.engine == other.engine

    def to_schema(self):
        return self.namespace + "/" + self.repository if self.namespace else self.repository

    __str__ = to_schema
    __repr__ = to_schema

    def __hash__(self):
        return hash(self.namespace) * hash(self.repository)

    checkout = sg.checkout
    uncheckout = sg.uncheckout
    commit = sg.commit
    get_all_hashes_tags = sg.get_all_hashes_tags
    resolve_image = sg.resolve_image
    import_tables = sg.import_tables
    get_parent_children = sg.get_parent_children

    def get_image(self, image_hash):
        image_tuple = sg.get_image(self, image=image_hash)
        return Image(*list(image_tuple), repository=self)

    def get_images(self):
        return [img[0] for img in sg.get_all_image_info(self)]

    push = sg.push
    pull = sg.pull
    has_pending_changes = _has_changes
    publish = sg.publish

    def get_head(self):
        return sg.get_current_head(self)

    def get_upstream(self):
        return get_upstream(self)

    def set_upstream(self, remote_name, remote_repository):
        set_upstream(self, remote_name, remote_repository)

    def run_sql(self, sql):
        engine = self.engine
        engine.run_sql("SET search_path TO %s", (self.to_schema(),))
        result = self.engine.run_sql(sql)
        engine.run_sql("SET search_path TO public")
        return result

    def diff(self, table_name, image_1, image_2=None, aggregate=False):
        return sg.diff(self, table_name, image_1, image_2, aggregate)


def to_repository(schema):
    """Converts a Postgres schema name of the format `namespace/repository` to a Splitgraph repository object."""
    if '/' in schema:
        namespace, repository = schema.split('/')
        return Repository(namespace, repository)
    return Repository('', schema)
