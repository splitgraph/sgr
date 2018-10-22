from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier

from splitgraph.constants import REGISTRY_META_SCHEMA
from splitgraph.meta_handler.common import insert, select


def _create_registry_schema(conn):
    """
    Creates the registry metadata schema that contains information on published images that will appear
    in the catalog.
    """
    with conn.cursor() as cur:
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


def ensure_registry_schema(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (REGISTRY_META_SCHEMA,))
        if cur.fetchone() is None:
            _create_registry_schema(conn)


def publish_tag(conn, repository, tag, image_hash, published, provenance, readme, schemata, previews, namespace=''):
    """
    Publishes a given tag in the remote catalog. Should't be called directly.
    Use splitgraph.commands.publish instead.

    :param conn: Psycopg connection object
    :param repository: Repository name
    :param tag: Tag to publish
    :param image_hash: Image hash corresponding to the given tag.
    :param published: Publish time (datetime)
    :param provenance: A list of tuples (repo_name, image_hash) showing what the image was created from
    :param readme: An optional README for the repo
    :param schemata: Dict mapping table name to a list of (column name, column type)
    :param previews: Dict mapping table name to a list of tuples with a preview
    :param namespace: Namespace (username or organisation) to publish the repository to.
    """
    with conn.cursor() as cur:
        cur.execute(insert("images",
                           ['namespace', 'repository', 'tag', 'image_hash', 'published',
                             'provenance', 'readme', 'schemata', 'previews'],
                           REGISTRY_META_SCHEMA), (namespace, repository, tag, image_hash, published, Json(provenance),
                                                   readme, Json(schemata), Json(previews)))


def get_published_info(conn, repository, tag, namespace=''):
    with conn.cursor() as cur:
        cur.execute(select("images",
                            'image_hash,published,provenance,readme,schemata,previews',
                            "namespace = %s AND repository = %s AND tag = %s",
                           REGISTRY_META_SCHEMA), (namespace, repository, tag))
        return cur.fetchone()


def unpublish_repository(conn, repository, namespace=''):
    with conn.cursor() as cur:
        cur.execute(SQL("DELETE FROM {}.{} WHERE namespace = %s AND repository = %s").format(Identifier(REGISTRY_META_SCHEMA),
                                                                          Identifier("images")), (namespace, repository))
