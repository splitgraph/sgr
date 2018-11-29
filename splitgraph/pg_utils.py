"""Various PG-specific functions that don't have any reference to SG"""

from psycopg2.sql import SQL, Identifier

from splitgraph._data.common import select


def pg_table_exists(conn, schema, table_name):
    # WTF: postgres quietly truncates all table names to 63 characters at creation and in select statements
    with conn.cursor() as cur:
        cur.execute("""SELECT table_name from information_schema.tables
                       WHERE table_schema = %s AND table_name = %s""", (schema, table_name[:63]))
        return cur.fetchone() is not None


def copy_table(conn, source_schema, source_table, target_schema, target_table, with_pk_constraints=True,
               table_exists=False):
    """
    Copies a table in the same Postgres instance, optionally applying primary key constraints as well.
    """
    if not table_exists:
        query = SQL("CREATE TABLE {}.{} AS SELECT * FROM {}.{};").format(
            Identifier(target_schema), Identifier(target_table),
            Identifier(source_schema), Identifier(source_table))
    else:
        query = SQL("INSERT INTO {}.{} SELECT * FROM {}.{};").format(
            Identifier(target_schema), Identifier(target_table),
            Identifier(source_schema), Identifier(source_table))
    if with_pk_constraints:
        pks = get_primary_keys(conn, source_schema, source_table)
        if pks:
            query += SQL("ALTER TABLE {}.{} ADD PRIMARY KEY (").format(
                Identifier(target_schema), Identifier(target_table)) + SQL(',').join(
                SQL("{}").format(Identifier(c)) for c, _ in pks) + SQL(")")

    with conn.cursor() as cur:
        cur.execute(query)


def dump_table_creation(conn, schema, tables, created_schema=None):
    """
    Dumps the basic table schema (column names, data types, is_nullable) for one or more tables into SQL statements.

    :param conn: psycopg connection object
    :param schema: Schema to dump tables from
    :param tables: Tables to dump
    :param created_schema: If not None, specifies the new schema that the tables will be created under.
    :return: An SQL statement that reconstructs the schema for the given tables.
    """
    queries = []

    with conn.cursor() as cur:
        for table in tables:
            cur.execute("""SELECT column_name, data_type, is_nullable
                           FROM information_schema.columns
                           WHERE table_name = %s AND table_schema = %s""", (table, schema))
            cols = cur.fetchall()
            if created_schema:
                target = SQL("{}.{}").format(Identifier(created_schema), Identifier(table))
            else:
                target = Identifier(table)
            query = SQL("CREATE TABLE {} (").format(target) + SQL(','.join(
                "{} %s " % ctype + ("NOT NULL" if not cnull else "") for _, ctype, cnull in cols)).format(
                *(Identifier(cname) for cname, _, _ in cols))

            pks = get_primary_keys(conn, schema, table)
            if pks:
                query += SQL(", PRIMARY KEY (") + SQL(',').join(SQL("{}").format(Identifier(c)) for c, _ in pks) + SQL(
                    "))")
            else:
                query += SQL(")")

            queries.append(query)
    return SQL(';').join(queries)


def create_table(conn, schema, table, schema_spec):
    """
    Creates a table using a previously-dumped table schema spec

    :param conn: psycopg connection object
    :param schema: Schema to create the table in
    :param table: Table name to create
    :param schema_spec: A list of (ordinal_position, column_name, data_type, is_pk) specifying the table schema
    """

    schema_spec = sorted(schema_spec)

    with conn.cursor() as cur:
        target = SQL("{}.{}").format(Identifier(schema), Identifier(table))
        query = SQL("CREATE TABLE {} (").format(target) + \
                SQL(','.join(
                    "{} %s " % ctype for _, _, ctype, _ in schema_spec)).format(
                    *(Identifier(cname) for _, cname, _, _ in schema_spec))

        pk_cols = [cname for _, cname, _, is_pk in schema_spec if is_pk]
        if pk_cols:
            query += SQL(", PRIMARY KEY (") + SQL(',').join(SQL("{}").format(Identifier(c)) for c in pk_cols) + SQL(
                "))")
        else:
            query += SQL(")")
        cur.execute(query)


def get_primary_keys(conn, schema, table):
    """Inspects the Postgres information_schema to get the primary keys for a given table."""
    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT a.attname, format_type(a.atttypid, a.atttypmod)
                           FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid
                                                                  AND a.attnum = ANY(i.indkey)
                           WHERE i.indrelid = '{}.{}'::regclass AND i.indisprimary""")
                    .format(Identifier(schema), Identifier(table)))
        return cur.fetchall()


def get_column_names(conn, schema, table_name):
    """Returns a list of all columns in a given table."""
    with conn.cursor() as cur:
        cur.execute("""SELECT column_name FROM information_schema.columns
                       WHERE table_schema = %s
                       AND table_name = %s
                       ORDER BY ordinal_position""", (schema, table_name))
        return [c[0] for c in cur.fetchall()]


def get_column_names_types(conn, schema, table_name):
    """Returns a list of (column, type) in a given table."""
    with conn.cursor() as cur:
        cur.execute("""SELECT column_name, data_type FROM information_schema.columns
                       WHERE table_schema = %s
                       AND table_name = %s""", (schema, table_name))
        return cur.fetchall()


def get_full_table_schema(conn, schema, table_name):
    """
    Generates a list of (column ordinal, name, data type, is_pk), used to detect schema changes like columns being
    dropped/added/renamed or type changes.
    """
    with conn.cursor() as cur:
        cur.execute("""SELECT ordinal_position, column_name, data_type FROM information_schema.columns
                       WHERE table_schema = %s
                       AND table_name = %s
                       ORDER BY ordinal_position""", (schema, table_name))
        results = cur.fetchall()

    # Do we need to make sure the PK has the same type + ordinal position here?
    pks = [pk for pk, _ in get_primary_keys(conn, schema, table_name)]
    return [(o, n, dt, (n in pks)) for o, n, dt in results]


def execute_sql_in(conn, schema, sql):
    """
    Executes a non-schema-qualified query against a specific schema, using PG's search_path.

    :param conn: psycopg connection object
    :param schema: Schema to run the query in
    :param sql: Query
    """
    with conn.cursor() as cur:
        # Execute the actual query against the original schema.
        cur.execute("SET search_path TO %s", (schema,))
        cur.execute(sql)
        cur.execute("SET search_path TO public")


def get_all_tables(conn, schema):
    """Gets all user tables in a schema (tracked or untracked)"""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s and table_type = 'BASE TABLE'""", (schema,))
        return [c[0] for c in cur.fetchall()]


def get_all_foreign_tables(conn, schema):
    """Inspects the information_schema to see which foreign tables we have in a given schema.
    Used by `import` to populate the metadata since if we did IMPORT FOREIGN SCHEMA we've no idea what tables we
    actually fetched from the mounted datablase"""
    with conn.cursor() as cur:
        cur.execute(
            select("tables", "table_name", "table_schema = %s and table_type = 'FOREIGN TABLE'", "information_schema"),
            (schema,))
        return [c[0] for c in cur.fetchall()]
