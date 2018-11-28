from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier

from splitgraph._data.common import insert, select
from splitgraph.config import REGISTRY_META_SCHEMA
from splitgraph.connection import get_connection


def _create_registry_schema():
    """
    Creates the registry metadata schema that contains information on published images that will appear
    in the catalog.
    """
    with get_connection().cursor() as cur:
        cur.execute(SQL("CREATE SCHEMA {}").format(Identifier(REGISTRY_META_SCHEMA)))
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        namespace  VARCHAR NOT NULL,
                        repository VARCHAR NOT NULL,
                        tag        VARCHAR NOT NULL,
                        image_hash VARCHAR NOT NULL,
                        published  TIMESTAMP,
                        provenance JSON,
                        readme     VARCHAR,
                        schemata   JSON,
                        previews   JSON,
                        PRIMARY KEY (namespace, repository, tag))""").format(Identifier(REGISTRY_META_SCHEMA),
                                                                             Identifier("images")))


def ensure_registry_schema():
    with get_connection().cursor() as cur:
        cur.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (REGISTRY_META_SCHEMA,))
        if cur.fetchone() is None:
            _create_registry_schema()


def publish_tag(repository, tag, image_hash, published, provenance, readme, schemata, previews):
    """
    Publishes a given tag in the remote catalog. Should't be called directly.
    Use splitgraph.commands.publish instead.

    :param repository: Repository name
    :param tag: Tag to publish
    :param image_hash: Image hash corresponding to the given tag.
    :param published: Publish time (datetime)
    :param provenance: A list of tuples (repository, image_hash) showing what the image was created from
    :param readme: An optional README for the repo
    :param schemata: Dict mapping table name to a list of (column name, column type)
    :param previews: Dict mapping table name to a list of tuples with a preview
    """
    with get_connection().cursor() as cur:
        cur.execute(insert("images",
                           ['namespace', 'repository', 'tag', 'image_hash', 'published',
                             'provenance', 'readme', 'schemata', 'previews'],
                           REGISTRY_META_SCHEMA), (repository.namespace, repository.repository, tag, image_hash,
                                                   published, Json(provenance), readme, Json(schemata), Json(previews)))


def get_published_info(repository, tag):
    with get_connection().cursor() as cur:
        cur.execute(select("images",
                            'image_hash,published,provenance,readme,schemata,previews',
                            "namespace = %s AND repository = %s AND tag = %s",
                           REGISTRY_META_SCHEMA), (repository.namespace, repository.repository, tag))
        return cur.fetchone()


def unpublish_repository(repository):
    with get_connection().cursor() as cur:
        cur.execute(SQL("DELETE FROM {}.{} WHERE namespace = %s AND repository = %s")
                    .format(Identifier(REGISTRY_META_SCHEMA), Identifier("images")),
                    (repository.namespace, repository.repository))
