from psycopg2.sql import SQL, Identifier

from splitgraph.constants import SPLITGRAPH_META_SCHEMA

META_TABLES = ['images', 'tags', 'objects', 'tables', 'remotes', 'object_locations', 'info']


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
                        CONSTRAINT sh_fk FOREIGN KEY (namespace, repository, image_hash) REFERENCES {}.{})""").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier("tags"),
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
                        CONSTRAINT tb_fk FOREIGN KEY (namespace, repository, image_hash) REFERENCES {}.{})""").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier("tables"),
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier("images")))

        # Keep track of what the remotes for a given repository are (by default, we create an "origin" remote
        # on initial pull)
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        namespace          VARCHAR NOT NULL,
                        repository         VARCHAR NOT NULL,
                        remote_name        VARCHAR NOT NULL,
                        remote_conn_string VARCHAR NOT NULL,
                        remote_namespace   VARCHAR NOT NULL,
                        remote_repository  VARCHAR NOT NULL,
                        PRIMARY KEY (namespace, repository, remote_name))""").format(
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

        # Miscellaneous key-value information for this driver (e.g. whether uploading objects is permitted etc).
        cur.execute(SQL("""CREATE TABLE {}.{} (
                        key   VARCHAR NOT NULL,
                        value VARCHAR NOT NULL,
                        PRIMARY KEY (key))""").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier("info")))


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


def get_info_key(conn, key):
    with conn.cursor() as cur:
        cur.execute(SQL("SELECT value FROM {}.info WHERE key = %s").format(Identifier(SPLITGRAPH_META_SCHEMA)), (key,))
        result = cur.fetchone()
        if result is None:
            return None
        return result[0]


def set_info_key(conn, key, value):
    with conn.cursor() as cur:
        cur.execute(SQL("INSERT INTO {0}.info (key, value) VALUES (%s, %s)"
                        " ON CONFLICT (key) DO UPDATE SET value = %s WHERE info.key = %s")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA)), (key, value, value, key))


_RLS_TABLES = ['images', 'tags', 'objects', 'tables']  # , 'object_locations', 'info']


def setup_registry_mode(conn):
    """
    Drops tables in splitgraph_meta that aren't pertinent to the registry + sets up access policies/RLS:

    * Normal users aren't allowed to create tables/schemata (can't do checkouts inside of a registry or
      upload SG objects directly to it)
    * images/tables/tags meta tables: can only create/update/delete records where the namespace = user ID
    * objects/object_location tables: same. An object (piece of data) becomes owned by the user that creates
      it and still remains so even if someone else's image starts using it. Hence, the original owner can delete
      or change it (since they control the external location they've uploaded it to anyway).

    :param conn: Psycopg admin connection object.
    """

    if get_info_key(conn, "registry_mode") == 'true':
        return

    with conn.cursor() as cur:
        cur.execute(SQL("REVOKE CREATE ON SCHEMA {} FROM PUBLIC").format(Identifier(SPLITGRAPH_META_SCHEMA)))
        cur.execute(SQL("GRANT USAGE ON SCHEMA {} TO PUBLIC").format(Identifier(SPLITGRAPH_META_SCHEMA)))
        cur.execute(SQL("REVOKE INSERT, DELETE, UPDATE ON TABLE {}.info FROM PUBLIC").format(
            Identifier(SPLITGRAPH_META_SCHEMA)))

        # Grant everything by default -- RLS will supersede these.
        cur.execute(SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {} TO PUBLIC")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA)))

        # Allow everyone to read objects that have been uploaded
        cur.execute(SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO PUBLIC")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA)))

        for t in _RLS_TABLES:
            cur.execute(SQL("ALTER TABLE {}.{} ENABLE ROW LEVEL SECURITY").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                                  Identifier(t)))
            cur.execute(SQL("""CREATE POLICY {2} ON {0}.{1} FOR SELECT USING (true)""")
                        .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(t), Identifier(t + '_S')))
            cur.execute(SQL("""CREATE POLICY {2} ON {0}.{1} FOR INSERT WITH CHECK ({0}.{1}.namespace = current_user)""")
                        .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(t), Identifier(t + '_I')))
            cur.execute(SQL("""CREATE POLICY {2} ON {0}.{1} FOR UPDATE USING (
                    {0}.{1}.namespace = current_user) WITH CHECK ({0}.{1}.namespace = current_user)""")
                        .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(t), Identifier(t + '_U')))
            cur.execute(SQL("""CREATE POLICY {2} ON {0}.{1} FOR DELETE USING (
                    {0}.{1}.namespace = current_user)""")
                        .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(t), Identifier(t + '_D')))

        # Object_locations is different, since we have to refer to the objects table for the namespace of the object
        # whose location we're changing.
        cur.execute(SQL("ALTER TABLE {}.{} ENABLE ROW LEVEL SECURITY").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                              Identifier('object_locations')))
        test_query = SQL("""(SELECT true FROM {0}.object_locations
                            JOIN {0}.objects ON {0}.object_locations.object_id = {0}.objects.object_id
                            WHERE {0}.objects.namespace = current_user)""".format(Identifier(SPLITGRAPH_META_SCHEMA)))

        cur.execute(SQL("""CREATE POLICY object_locations_S ON {}.object_locations FOR SELECT USING (true)""")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA)))
        cur.execute(SQL("""CREATE POLICY object_locations_I ON {}.object_locations FOR INSERT
                           WITH CHECK """).format(Identifier(SPLITGRAPH_META_SCHEMA)) + test_query)
        cur.execute(SQL("CREATE POLICY object_locations_U ON {}.object_locations FOR UPDATE USING ")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA)) + test_query + SQL(" CHECK ") + test_query)
        cur.execute(SQL("""CREATE POLICY object_locations_D ON {}.object_locations FOR DELETE
                           USING """).format(Identifier(SPLITGRAPH_META_SCHEMA)) + test_query)

        set_info_key(conn, "registry_mode", "true")


def toggle_registry_rls(conn, mode='ENABLE'):
    # For testing purposes: switch RLS off so that we can add test data that we then won't be able to alter when
    # switching it on.
    # Modes: ENABLE is same as FORCE, but doesn't apply to the table owner.

    if mode not in ('ENABLE', 'DISABLE', 'FORCE'):
        raise ValueError()

    with conn.cursor() as cur:
        for t in _RLS_TABLES:
            cur.execute(SQL("ALTER TABLE {}.{} %s ROW LEVEL SECURITY" % mode).format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                                     Identifier(t)))
