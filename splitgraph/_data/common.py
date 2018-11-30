"""
Common internal metadata access functions
"""

from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.connection import get_connection

META_TABLES = ['images', 'tags', 'objects', 'tables', 'upstream', 'object_locations', 'info']


def _create_metadata_schema():
    """
    Creates the metadata schema splitgraph_meta that stores the hash tree of schema snaps and the current tags.
    This means we can't mount anything under the schema splitgraph_meta -- much like we can't have a folder
    ".git" under Git version control...

    This all should probably be moved into some sort of a routine that runs when the whole driver is set up
    for the first time.
    """

    with get_connection().cursor() as cur:
        cur.execute(SQL("CREATE SCHEMA {}").format(Identifier(SPLITGRAPH_META_SCHEMA)))
        # maybe FK parent_id on image_hash. NULL there means this is the repo root.
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        namespace       VARCHAR NOT NULL,
                        repository      VARCHAR NOT NULL,
                        image_hash      VARCHAR NOT NULL,
                        parent_id       VARCHAR,
                        created         TIMESTAMP,
                        comment         VARCHAR,
                        provenance_type VARCHAR,
                        provenance_data JSON,
                        PRIMARY KEY (namespace, repository, image_hash))""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                                    Identifier("images")))
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        namespace       VARCHAR NOT NULL,
                        repository      VARCHAR NOT NULL,
                        image_hash VARCHAR,
                        tag        VARCHAR,
                        PRIMARY KEY (namespace, repository, tag),
                        CONSTRAINT sh_fk FOREIGN KEY (namespace, repository, image_hash) REFERENCES {}.{})""")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("tags"),
                            Identifier(SPLITGRAPH_META_SCHEMA), Identifier("images")))

        # A tree of object parents. The parent of an object is not necessarily the object linked to
        # the parent commit that the object belongs to (e.g. if we imported the object from a different commit tree).
        # One object can have multiple parents (e.g. 1 SNAP and 1 DIFF).
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        object_id  VARCHAR NOT NULL,
                        namespace  VARCHAR NOT NULL,
                        format     VARCHAR NOT NULL,
                        parent_id  VARCHAR)""").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("objects")))

        # Maps a given table at a given point in time to an "object ID" (either a full snapshot or a
        # delta to a previous table).
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        namespace  VARCHAR NOT NULL,
                        repository VARCHAR NOT NULL,
                        image_hash VARCHAR NOT NULL,
                        table_name VARCHAR NOT NULL,
                        object_id  VARCHAR NOT NULL,
                        PRIMARY KEY (namespace, repository, image_hash, table_name, object_id),
                        CONSTRAINT tb_fk FOREIGN KEY (namespace, repository, image_hash) REFERENCES {}.{})""")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("tables"),
                            Identifier(SPLITGRAPH_META_SCHEMA), Identifier("images")))

        # Keep track of what the remotes for a given repository are (by default, we create an "origin" remote
        # on initial pull)
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        namespace          VARCHAR NOT NULL,
                        repository         VARCHAR NOT NULL,
                        remote_name        VARCHAR NOT NULL,
                        remote_namespace   VARCHAR NOT NULL,
                        remote_repository  VARCHAR NOT NULL,
                        PRIMARY KEY (namespace, repository))""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                        Identifier("upstream")))

        # Map objects to their locations for when they don't live on the remote or the local machine but instead
        # in S3/some FTP/HTTP server/torrent etc.
        # Lookup path to resolve an object on checkout: local -> this table -> remote (so that we don't bombard
        # the remote with queries for tables that may have been uploaded to a different place).
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        object_id          VARCHAR NOT NULL,
                        location           VARCHAR NOT NULL,
                        protocol           VARCHAR NOT NULL,
                        PRIMARY KEY (object_id))""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                            Identifier("object_locations")))

        # Miscellaneous key-value information for this driver (e.g. whether uploading objects is permitted etc).
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        key   VARCHAR NOT NULL,
                        value VARCHAR NOT NULL,
                        PRIMARY KEY (key))""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                      Identifier("info")))


def select(table, columns='*', where='', schema=SPLITGRAPH_META_SCHEMA):
    """
    A generic SQL SELECT constructor to simplify metadata access queries so that we don't have to repeat the same
    identifiers everywhere.

    :param table: Table to select from.
    :param columns: Columns to select as a string. WARN: concatenated directly without any formatting.
    :param where: If specified, added to the query with a "WHERE" keyword. WARN also concatenated directly.
    :param schema: Defaults to SPLITGRAPH_META_SCHEMA.
    :return: A psycopg2.sql.SQL object with the query.
    """
    query = SQL("SELECT " + columns + " FROM {}.{}").format(Identifier(schema), Identifier(table))
    if where:
        query += SQL(" WHERE " + where)
    return query


def insert(table, columns, schema=SPLITGRAPH_META_SCHEMA):
    """
    A generic SQL SELECT constructor to simplify metadata access queries so that we don't have to repeat the same
    identifiers everywhere.

    :param table: Table to select from.
    :param columns: Columns to insert as a list of strings.
    :return: A psycopg2.sql.SQL object with the query (parameterized)
    """
    query = SQL("INSERT INTO {}.{}").format(Identifier(schema), Identifier(table))
    query += SQL("(" + ",".join("{}" for _ in columns) + ")").format(*map(Identifier, columns))
    query += SQL("VALUES (" + ','.join("%s" for _ in columns) + ")")
    return query


def ensure_metadata_schema():
    """Create the metadata schema if it doesn't exist"""
    with get_connection().cursor() as cur:
        cur.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (SPLITGRAPH_META_SCHEMA,))
        if cur.fetchone() is None:
            _create_metadata_schema()
