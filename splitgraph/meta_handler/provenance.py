from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier

from splitgraph.constants import SPLITGRAPH_META_SCHEMA

_QUERY = SQL("""UPDATE {}.images SET provenance_type = %s, provenance_data = %s WHERE
                            namespace = %s AND repository = %s AND image_hash = %s""")\
    .format(Identifier(SPLITGRAPH_META_SCHEMA))


def store_import_provenance(conn, repository, image_hash, source_repository, source_hash,
                            tables, table_aliases, table_queries):
    with conn.cursor() as cur:
        cur.execute(_QUERY,
                    ("IMPORT", Json({
                        'source': source_repository.repository,
                        'source_namespace': source_repository.namespace,
                        'source_hash': source_hash,
                        'tables': tables,
                        'table_aliases': table_aliases,
                        'table_queries': table_queries}), repository.namespace, repository.repository, image_hash))


def store_sql_provenance(conn, repository, image_hash, sql):
    with conn.cursor() as cur:
        cur.execute(_QUERY, ('SQL', Json(sql), repository.namespace, repository.repository, image_hash))


def store_mount_provenance(conn, repository, image_hash):
    # We don't store the details of images that come from an sgr MOUNT command since those are assumed to be based
    # on an inaccessible db
    with conn.cursor() as cur:
        cur.execute(_QUERY, ('MOUNT', None, repository.namespace, repository.repository, image_hash))


def store_from_provenance(conn, repository, image_hash, source):
    with conn.cursor() as cur:
        cur.execute(_QUERY, ('FROM', Json({'source': source.repository, 'source_namespace': source.namespace}),
                             repository.namespace, repository.repository, image_hash))
