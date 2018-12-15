from enum import Enum

from psycopg2.sql import SQL, Identifier


class ResultShape(Enum):
    """Shape that the result of a query will be coerced to"""
    NONE = 0  # No result expected
    ONE_ONE = 1  # e.g. "row1_val1"
    ONE_MANY = 2  # e.g. ("row1_val1", "row1_val_2")
    MANY_ONE = 3  # e.g. ["row1_val1", "row2_val_1", ...]
    MANY_MANY = 4  # e.g. [("row1_val1", "row1_val_2"), ("row2_val1", "row2_val_2"), ...]


class Engine:
    """Abstraction for a Splitgraph SQL backend"""

    def run_sql(self, statement, arguments=(), return_shape=ResultShape.MANY_MANY):
        """Run an arbitrary SQL statement with some arguments, return an iterator of results"""
        raise NotImplemented()

    def execute_sql_in(self, schema, sql):
        """
        Executes a non-schema-qualified query against a specific schema, using PG's search_path.

        :param schema: Schema to run the query in
        :param sql: Query
        """
        self.run_sql("SET search_path TO %s", (schema,), return_shape=ResultShape.NONE)
        self.run_sql(sql, return_shape=ResultShape.NONE)
        self.run_sql("SET search_path TO public", (schema,), return_shape=ResultShape.NONE)

    def table_exists(self, schema, table_name):
        """
        Check if a table exists on the engine
        :param schema: Schema name
        :param table_name: Table name
        """
        return self.run_sql("""SELECT table_name from information_schema.tables
                           WHERE table_schema = %s AND table_name = %s""", (schema, table_name[:63]),
                            ResultShape.ONE_ONE) is not None

    def schema_exists(self, schema):
        """
        Check if a schema exists on the engine
        :param schema: Schema name
        """
        return self.run_sql("""SELECT 1 from information_schema.schemata
                           WHERE schema_name = %s""", (schema,), return_shape=ResultShape.ONE_ONE) is not None

    def copy_table(self, source_schema, source_table, target_schema, target_table, with_pk_constraints=True,
                   table_exists=False):
        """
        Copies a table in the same engine , optionally applying primary key constraints as well.
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
            pks = self.get_primary_keys(source_schema, source_table)
            if pks:
                query += SQL("ALTER TABLE {}.{} ADD PRIMARY KEY (").format(
                    Identifier(target_schema), Identifier(target_table)) + SQL(',').join(
                    SQL("{}").format(Identifier(c)) for c, _ in pks) + SQL(")")
        self.run_sql(query, return_shape=ResultShape.NONE)

    def get_all_tables(self, schema):
        """Get all tables in a given schema"""
        return self.run_sql("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (schema,),
                            return_shape=ResultShape.MANY_ONE)

    def get_primary_keys(self, schema, table):
        """Get a list of (column_name, column_type) denoting the primary keys of a given table."""
        raise NotImplemented()

    def dump_table_creation(self, schema, tables, created_schema=None):
        """
        Dumps the basic table schema (column names, data types, is_nullable) for one or more tables into SQL statements.

        :param schema: Schema to dump tables from
        :param tables: Tables to dump
        :param created_schema: If not None, specifies the new schema that the tables will be created under.
        :return: An SQL statement that reconstructs the schema for the given tables.
        """
        queries = []

        for table in tables:
            cols = self.run_sql("""SELECT column_name, data_type, is_nullable
                           FROM information_schema.columns
                           WHERE table_name = %s AND table_schema = %s""", (table, schema))
            if created_schema:
                target = SQL("{}.{}").format(Identifier(created_schema), Identifier(table))
            else:
                target = Identifier(table)
            query = SQL("CREATE TABLE {} (").format(target) + SQL(','.join(
                "{} %s " % ctype + ("NOT NULL" if not cnull else "") for _, ctype, cnull in cols)).format(
                *(Identifier(cname) for cname, _, _ in cols))

            pks = self.get_primary_keys(schema, table)
            if pks:
                query += SQL(", PRIMARY KEY (") + SQL(',').join(SQL("{}").format(Identifier(c)) for c, _ in pks) + SQL(
                    "))")
            else:
                query += SQL(")")

            queries.append(query)
        return SQL(';').join(queries)

    def create_table(self, schema, table, schema_spec):
        """
        Creates a table using a previously-dumped table schema spec

        :param schema: Schema to create the table in
        :param table: Table name to create
        :param schema_spec: A list of (ordinal_position, column_name, data_type, is_pk) specifying the table schema
        """

        schema_spec = sorted(schema_spec)

        target = SQL("{}.{}").format(Identifier(schema), Identifier(table))
        query = SQL("CREATE TABLE {} (").format(target) \
                + SQL(','.join("{} %s " % ctype for _, _, ctype, _ in schema_spec)) \
                    .format(*(Identifier(cname) for _, cname, _, _ in schema_spec))

        pk_cols = [cname for _, cname, _, is_pk in schema_spec if is_pk]
        if pk_cols:
            query += SQL(", PRIMARY KEY (") + SQL(',').join(SQL("{}").format(Identifier(c)) for c in pk_cols) + SQL(
                "))")
        else:
            query += SQL(")")
        self.run_sql(query, return_shape=ResultShape.NONE)

    def get_column_names(self, schema, table_name):
        """Returns a list of all columns in a given table."""
        return self.run_sql("""SELECT column_name FROM information_schema.columns
                           WHERE table_schema = %s
                           AND table_name = %s
                           ORDER BY ordinal_position""", (schema, table_name), return_shape=ResultShape.MANY_ONE)

    def get_column_names_types(self, schema, table_name):
        """Returns a list of (column, type) in a given table."""
        return self.run_sql("""SELECT column_name, data_type FROM information_schema.columns
                           WHERE table_schema = %s
                           AND table_name = %s""", (schema, table_name))

    def get_full_table_schema(self, schema, table_name):
        """
        Generates a list of (column ordinal, name, data type, is_pk), used to detect schema changes like columns being
        dropped/added/renamed or type changes.
        """
        results = self.run_sql("""SELECT ordinal_position, column_name, data_type FROM information_schema.columns
                           WHERE table_schema = %s
                           AND table_name = %s
                           ORDER BY ordinal_position""", (schema, table_name))

        # Do we need to make sure the PK has the same type + ordinal position here?
        pks = [pk for pk, _ in self.get_primary_keys(schema, table_name)]
        return [(o, n, dt, (n in pks)) for o, n, dt in results]


_ENGINE = None


def get_engine():
    global _ENGINE
    if not _ENGINE:
        from .postgres.engine import PostgresEngine
        _ENGINE = PostgresEngine()
    return _ENGINE
