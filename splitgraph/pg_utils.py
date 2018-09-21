"""Various PG-specific functions that don't have any reference to SG"""

from psycopg2.sql import SQL, Identifier


def pg_table_exists(conn, mountpoint, table_name):
    # WTF: postgres quietly truncates all table names to 63 characters at creation and in select statements
    with conn.cursor() as cur:
        cur.execute("""SELECT table_name from information_schema.tables
                       WHERE table_schema = %s AND table_name = %s""", (mountpoint, table_name[:63]))
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
            query = SQL("CREATE TABLE {} (").format(target) + \
                    SQL(','.join(
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


def get_primary_keys(conn, mountpoint, table):
    """Inspects the Postgres information_schema to get the primary keys for a given table."""
    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT a.attname, format_type(a.atttypid, a.atttypmod)
                           FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid
                                                                  AND a.attnum = ANY(i.indkey)
                           WHERE i.indrelid = '{}.{}'::regclass AND i.indisprimary""").format(
            Identifier(mountpoint), Identifier(table)))
        return cur.fetchall()


def _get_column_names(conn, mountpoint, table_name):
    with conn.cursor() as cur:
        cur.execute("""SELECT column_name FROM information_schema.columns
                       WHERE table_schema = %s
                       AND table_name = %s
                       ORDER BY ordinal_position""", (mountpoint, table_name))
        return [c[0] for c in cur.fetchall()]


def get_column_names_types(conn, mountpoint, table_name):
    with conn.cursor() as cur:
        cur.execute("""SELECT column_name, data_type FROM information_schema.columns
                       WHERE table_schema = %s
                       AND table_name = %s""", (mountpoint, table_name))
        return cur.fetchall()


def get_full_table_schema(conn, mountpoint, table_name):
    # Generates a list of (column ordinal, name, data type, is_pk), used to detect
    # schema changes like columns being dropped/added/renamed or type changes.
    with conn.cursor() as cur:
        cur.execute("""SELECT ordinal_position, column_name, data_type FROM information_schema.columns
                       WHERE table_schema = %s
                       AND table_name = %s
                       ORDER BY ordinal_position""", (mountpoint, table_name))
        results = cur.fetchall()

    # Do we need to make sure the PK has the same type + ordinal position here?
    pks = [pk for pk, _ in get_primary_keys(conn, mountpoint, table_name)]
    return [(o, n, dt, (n in pks)) for o, n, dt in results]


def table_dump_generator(conn, schema, table):
    """Returns an iterator that generates a SQL table dump, non-schema qualified."""
    # Don't include a schema (mountpoint) qualifier since the dump might be imported into a different place.
    yield (dump_table_creation(conn, schema, [table], created_schema=None) + SQL(';\n')).as_string(conn)

    # Use a server-side cursor here so we don't fetch the whole db into memory immediately.
    with conn.cursor(name='sg_table_upload_cursor') as cur:
        cur.itersize = 10000
        cur.execute(SQL("SELECT * FROM {}.{}""").format(Identifier(schema), Identifier(table)))
        row = next(cur)
        q = '(' + ','.join('%s' for _ in row) + ')'
        yield cur.mogrify(SQL("INSERT INTO {} VALUES " + q).format(Identifier(table)), row).decode('utf-8')
        for row in cur:
            yield cur.mogrify(',' + q, row).decode('utf-8')
        yield ';\n'
    return


def execute_sql_in(conn, mountpoint, sql):
    """
    Executes a non-schema-qualified query against a specific schema, using PG's search_path.

    :param conn: psycopg connection object
    :param mountpoint: Schema to run the query in
    :param sql: Query
    """
    with conn.cursor() as cur:
        # Execute the actual query against the original mountpoint.
        cur.execute("SET search_path TO %s", (mountpoint,))
        cur.execute(sql)
        cur.execute("SET search_path TO public")
