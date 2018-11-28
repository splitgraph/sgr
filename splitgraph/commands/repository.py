from collections.__init__ import namedtuple

from psycopg2.sql import SQL, Identifier

from splitgraph import get_connection
from splitgraph._data.common import ensure_metadata_schema
from splitgraph._data.misc import register_repository, unregister_repository
from splitgraph.commands._pg_audit import manage_audit, discard_pending_changes


class Repository(namedtuple('Repository', ['namespace', 'repository'])):
    """
    A repository object that encapsulates the namespace and the actual repository name.
    """

    def to_schema(self):
        """Converts the object to a Postgres schema."""
        return self.repository if not self.namespace else self.namespace + '/' + self.repository

    __repr__ = to_schema
    __str__ = to_schema


def to_repository(schema):
    """Converts a Postgres schema name of the format `namespace/repository` to a Splitgraph repository object."""
    if '/' in schema:
        namespace, repository = schema.split('/')
        return Repository(namespace, repository)
    return Repository('', schema)


@manage_audit
def init(repository):
    """
    Initializes an empty repo with an initial commit (hash 0000...)

    :param repository: Repository to create. Must not exist locally.
    """
    with get_connection().cursor() as cur:
        cur.execute(SQL("CREATE SCHEMA {}").format(Identifier(repository.to_schema())))
    image_hash = '0' * 64
    register_repository(repository, image_hash, tables=[], table_object_ids=[])


def unmount(repository):
    """
    Discards all changes to a given repository and all of its history, deleting the physical Postgres schema.
    Doesn't delete any cached physical objects.

    :param repository: Repository to unmount.
    :return:
    """
    # Make sure to discard changes to this repository if they exist, otherwise they might
    # be applied/recorded if a new repository with the same name appears.
    ensure_metadata_schema()
    discard_pending_changes(repository.to_schema())
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute(SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(repository.to_schema())))
        # Drop server too if it exists (could have been a non-foreign repository)
        cur.execute(SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(repository.to_schema() + '_server')))

    # Currently we just discard all history info about the mounted schema
    unregister_repository(repository)
    conn.commit()
