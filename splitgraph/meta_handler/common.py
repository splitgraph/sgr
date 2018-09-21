from psycopg2.sql import SQL, Identifier

from splitgraph.constants import SPLITGRAPH_META_SCHEMA

META_TABLES = ['snap_tree', 'snap_tags', 'object_tree', 'tables', 'remotes', 'object_locations', 'pending_changes']


def _create_metadata_schema(conn):
    """
    Creates the metadata schema splitgraph_meta that stores the hash tree of schema snaps and the current tags.
    This means we can't mount anything under the schema splitgraph_meta -- much like we can't have a folder
    ".git" under Git version control...

    This all should probably be moved into some sort of a routine that runs when the whole driver is set up
    for the first time.
    """

    with conn.cursor() as cur:
        cur.execute(SQL("CREATE SCHEMA {}").format(Identifier(SPLITGRAPH_META_SCHEMA)))
        # maybe FK parent_id on snap_id. NULL there means this is the repo root.
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        snap_id         VARCHAR NOT NULL,
                        mountpoint      VARCHAR NOT NULL,
                        parent_id       VARCHAR,
                        created         TIMESTAMP,
                        comment         VARCHAR,
                        provenance_type VARCHAR,
                        provenance_data JSON,
                        PRIMARY KEY (mountpoint, snap_id))""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                      Identifier("snap_tree")))
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        mountpoint VARCHAR NOT NULL,
                        snap_id    VARCHAR,
                        tag        VARCHAR,
                        PRIMARY KEY (mountpoint, tag),
                        CONSTRAINT sh_fk FOREIGN KEY (mountpoint, snap_id) REFERENCES {}.{})""").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier("snap_tags"),
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier("snap_tree")))

        # A tree of object parents. The parent of an object is not necessarily the object linked to
        # the parent commit that the object belongs to (e.g. if we imported the object from a different commit tree).
        # One object can have multiple parents (e.g. 1 SNAP and 1 DIFF).
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        object_id  VARCHAR NOT NULL,
                        format     VARCHAR NOT NULL,
                        parent_id  VARCHAR)""").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("object_tree")))

        # Maps a given table at a given point in time to an "object ID" (either a full snapshot or a
        # delta to a previous table).
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        mountpoint VARCHAR NOT NULL,
                        snap_id    VARCHAR NOT NULL,
                        table_name VARCHAR NOT NULL,
                        object_id  VARCHAR NOT NULL,
                        PRIMARY KEY (mountpoint, snap_id, table_name, object_id),
                        CONSTRAINT tb_fk FOREIGN KEY (mountpoint, snap_id) REFERENCES {}.{})""").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier("tables"),
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier("snap_tree")))

        # Keep track of what the remotes for a given mountpoint are (by default, we create an "origin" remote
        # on initial pull)
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        mountpoint         VARCHAR NOT NULL,
                        remote_name        VARCHAR NOT NULL,
                        remote_conn_string VARCHAR NOT NULL,
                        remote_mountpoint  VARCHAR NOT NULL,
                        PRIMARY KEY (mountpoint, remote_name))""").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier("remotes")))

        # Map objects to their locations for when they don't live on the remote or the local machine but instead
        # in S3/some FTP/HTTP server/torrent etc.
        # Lookup path to resolve an object on checkout: local -> this table -> remote (so that we don't bombard
        # the remote with queries for tables that may have been uploaded to a different place).
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        object_id          VARCHAR NOT NULL,
                        location           VARCHAR NOT NULL,
                        protocol           VARCHAR NOT NULL,
                        PRIMARY KEY (object_id))""").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier("object_locations")))


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


def ensure_metadata_schema(conn):
    # Check if the metadata schema actually exists.
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (SPLITGRAPH_META_SCHEMA,))
        if cur.fetchone() is None:
            _create_metadata_schema(conn)
