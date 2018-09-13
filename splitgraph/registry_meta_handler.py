import json

from psycopg2.sql import SQL, Identifier

from splitgraph.constants import REGISTRY_META_SCHEMA
from splitgraph.meta_handler import _insert, _select


def _create_registry_schema(conn):
    """
    Creates the registry metadata schema that contains information on published images that will appear
    in the catalog.
    """
    with conn.cursor() as cur:
        cur.execute(SQL("CREATE SCHEMA {}").format(Identifier(REGISTRY_META_SCHEMA)))
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        repository VARCHAR NOT NULL,
                        tag        VARCHAR NOT NULL,
                        image_hash VARCHAR NOT NULL,
                        published  TIMESTAMP,
                        provenance VARCHAR,
                        readme     VARCHAR NOT NULL,
                        PRIMARY KEY (repository, tag))""").format(Identifier(REGISTRY_META_SCHEMA),
                                                                  Identifier("images")))


def ensure_registry_schema(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (REGISTRY_META_SCHEMA,))
        if cur.fetchone() is None:
            _create_registry_schema(conn)


def publish_tag(conn, repository, tag, image_hash, published, provenance, readme):
    with conn.cursor() as cur:
        cur.execute(_insert("images",
                            ['repository', 'tag', 'image_hash', 'published', 'provenance', 'readme'],
                            REGISTRY_META_SCHEMA), (repository, tag, image_hash, published, json.dumps(provenance),
                                                    readme))


def get_published_info(conn, repository, tag):
    with conn.cursor() as cur:
        cur.execute(_select("images",
                            'image_hash,published,provenance,readme',
                            "repository = %s AND tag = %s",
                            REGISTRY_META_SCHEMA), (repository, tag))
        image_hash, published, provenance, readme = cur.fetchone()
        return image_hash, published, json.loads(provenance), readme


def unpublish_repository(conn, repository):
    with conn.cursor() as cur:
        cur.execute(SQL("DELETE FROM {}.{} WHERE repository = %s").format(Identifier(REGISTRY_META_SCHEMA),
                                                                          Identifier("images")), (repository,))
