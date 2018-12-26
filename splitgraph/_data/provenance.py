"""
Internal metadata functions used by the Splitfile interpreter to store provenance
"""

from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA

_QUERY = SQL("""UPDATE {}.images SET provenance_type = %s, provenance_data = %s WHERE
                            namespace = %s AND repository = %s AND image_hash = %s""") \
    .format(Identifier(SPLITGRAPH_META_SCHEMA))


def store_import_provenance(repository, image_hash, source_repository, source_hash, tables, table_aliases,
                            table_queries):
    """Stores provenance for images produced by the FROM ... IMPORT Splitfile statement."""
    repository.engine.run_sql(_QUERY,
                              ("IMPORT", Json({
                                  'source': source_repository.repository,
                                  'source_namespace': source_repository.namespace,
                                  'source_hash': source_hash,
                                  'tables': tables,
                                  'table_aliases': table_aliases,
                                  'table_queries': table_queries}), repository.namespace, repository.repository,
                               image_hash))


def store_sql_provenance(repository, image_hash, sql):
    """Stores provenance for images produced by the SQL Splitfile statement."""
    repository.engine.run_sql(_QUERY, ('SQL', Json(sql), repository.namespace, repository.repository, image_hash))


def store_mount_provenance(repository, image_hash):
    """Stores provenance for images produced by the FROM MOUNT Splitfile statement.

    We don't store the details of images that come from an sgr MOUNT command since those are assumed to be based
    on an inaccessible db.
    """
    repository.engine.run_sql(_QUERY, ('MOUNT', None, repository.namespace, repository.repository, image_hash))


def store_from_provenance(repository, image_hash, source):
    """Stores provenance for images produced by the FROM... Splitfile statement."""
    repository.engine.run_sql(_QUERY,
                              ('FROM', Json({'source': source.repository, 'source_namespace': source.namespace}),
                               repository.namespace, repository.repository, image_hash))
