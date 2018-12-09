"""
Functions for communicating with the remote Splitgraph catalog
"""

from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier

from splitgraph._data.common import insert, select
from splitgraph.config import REGISTRY_META_SCHEMA, SPLITGRAPH_META_SCHEMA
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


def _ensure_registry_schema():
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
    """
    Get information on an image that's published in a catalog. Should be called with the driver connection
    switching context manager (`override_driver_connection`).

    :param repository: Repository
    :param tag: Image tag
    :return: A tuple of (image_hash, published_timestamp, provenance, readme, table schemata, previews)
    """
    with get_connection().cursor() as cur:
        cur.execute(select("images",
                           'image_hash,published,provenance,readme,schemata,previews',
                           "namespace = %s AND repository = %s AND tag = %s",
                           REGISTRY_META_SCHEMA), (repository.namespace, repository.repository, tag))
        return cur.fetchone()


def unpublish_repository(repository):
    """
    Deletes the repository from the remote catalog. Should be called with the driver connection
    switching context manager (`override_driver_connection`).

    :param repository: Repository to unpublish
    """
    with get_connection().cursor() as cur:
        cur.execute(SQL("DELETE FROM {}.{} WHERE namespace = %s AND repository = %s")
                    .format(Identifier(REGISTRY_META_SCHEMA), Identifier("images")),
                    (repository.namespace, repository.repository))


def get_info_key(key):
    """
    Gets a configuration key from the remote registry, used to notify the client of the registry's capabilities.

    Should be called with the driver connection switching context manager (`override_driver_connection`).

    :param key: Key to get
    :return:
    """
    with get_connection().cursor() as cur:
        cur.execute(SQL("SELECT value FROM {}.info WHERE key = %s").format(Identifier(SPLITGRAPH_META_SCHEMA)), (key,))
        result = cur.fetchone()
        if result is None:
            return None
        return result[0]


def set_info_key(key, value):
    """
    Sets a configuration value on the remote registry.

    Should be called with the driver connection switching context manager (`override_driver_connection`).

    :param key: Key to set
    :param value: New value for the key
    :return:
    """
    with get_connection().cursor() as cur:
        cur.execute(SQL("INSERT INTO {0}.info (key, value) VALUES (%s, %s)"
                        " ON CONFLICT (key) DO UPDATE SET value = excluded.value WHERE info.key = excluded.key")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA)), (key, value))


_RLS_TABLES = ['images', 'tags', 'objects', 'tables']


def setup_registry_mode():
    """
    Drops tables in splitgraph_meta that aren't pertinent to the registry + sets up access policies/RLS:

    * Normal users aren't allowed to create tables/schemata (can't do checkouts inside of a registry or
      upload SG objects directly to it)
    * images/tables/tags meta tables: can only create/update/delete records where the namespace = user ID
    * objects/object_location tables: same. An object (piece of data) becomes owned by the user that creates
      it and still remains so even if someone else's image starts using it. Hence, the original owner can delete
      or change it (since they control the external location they've uploaded it to anyway).

    """

    if get_info_key("registry_mode") == 'true':
        return

    with get_connection().cursor() as cur:
        for schema in (SPLITGRAPH_META_SCHEMA, REGISTRY_META_SCHEMA):
            cur.execute(SQL("REVOKE CREATE ON SCHEMA {} FROM PUBLIC").format(Identifier(schema)))
            cur.execute(SQL("GRANT USAGE ON SCHEMA {} TO PUBLIC").format(Identifier(schema)))
            # Grant everything by default -- RLS will supersede these.
            cur.execute(SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {} TO PUBLIC")
                        .format(Identifier(schema)))

        cur.execute(SQL("REVOKE INSERT, DELETE, UPDATE ON TABLE {}.info FROM PUBLIC").format(
            Identifier(SPLITGRAPH_META_SCHEMA)))

        # Allow everyone to read objects that have been uploaded
        cur.execute(SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO PUBLIC")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA)))

        for t in _RLS_TABLES:
            _setup_rls_policies(cur, t)

        # Object_locations is different, since we have to refer to the objects table for the namespace of the object
        # whose location we're changing.
        test_query = """((SELECT true as bool FROM {0}.{1}
                            JOIN {0}.objects ON {0}.{1}.object_id = {0}.objects.object_id
                            WHERE {0}.objects.namespace = current_user) = true)"""
        _setup_rls_policies(cur, "object_locations", condition=test_query)
        _setup_rls_policies(cur, "images", schema=REGISTRY_META_SCHEMA)

        set_info_key("registry_mode", "true")


def _setup_rls_policies(cursor, table, schema=SPLITGRAPH_META_SCHEMA, condition=None):
    condition = condition or "({0}.{1}.namespace = current_user)"

    cursor.execute(SQL("ALTER TABLE {}.{} ENABLE ROW LEVEL SECURITY").format(Identifier(schema),
                                                                             Identifier(table)))
    for flavour in 'SIUD':
        cursor.execute(SQL("DROP POLICY IF EXISTS {2} ON {0}.{1}")
                       .format(Identifier(schema), Identifier(table), Identifier(table + '_' + flavour)))
    cursor.execute(SQL("""CREATE POLICY {2} ON {0}.{1} FOR SELECT USING (true)""")
                   .format(Identifier(schema), Identifier(table), Identifier(table + '_S')))
    cursor.execute(SQL("""CREATE POLICY {2} ON {0}.{1} FOR INSERT WITH CHECK """)
                   .format(Identifier(schema), Identifier(table), Identifier(table + '_I'))
                   + SQL(condition).format(Identifier(schema), Identifier(table)))
    cursor.execute(SQL("CREATE POLICY {2} ON {0}.{1} FOR UPDATE USING ")
                   .format(Identifier(schema), Identifier(table), Identifier(table + '_U'))
                   + SQL(condition).format(Identifier(schema), Identifier(table))
                   + SQL(" WITH CHECK ") + SQL(condition).format(Identifier(schema), Identifier(table)))
    cursor.execute(SQL("CREATE POLICY {2} ON {0}.{1} FOR DELETE USING ")
                   .format(Identifier(schema), Identifier(table), Identifier(table + '_D'))
                   + SQL(condition).format(Identifier(schema), Identifier(table)))


def toggle_registry_rls(mode='ENABLE'):
    """
    Switches row-level security on the registry, restricting write access to metadata tables
    to owners of relevant repositories/objects.

    :param mode: ENABLE, DISABLE or FORCE (enable for superusers/table owners)
    :return:
    """

    if mode not in ('ENABLE', 'DISABLE', 'FORCE'):
        raise ValueError()

    with get_connection().cursor() as cur:
        for t in _RLS_TABLES:
            cur.execute(SQL("ALTER TABLE {}.{} %s ROW LEVEL SECURITY" % mode).format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                                     Identifier(t)))
        cur.execute(SQL("ALTER TABLE {}.{} %s ROW LEVEL SECURITY" % mode).format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                                 Identifier("object_locations")))
        cur.execute(SQL("ALTER TABLE {}.{} %s ROW LEVEL SECURITY" % mode).format(Identifier(REGISTRY_META_SCHEMA),
                                                                                 Identifier("images")))
