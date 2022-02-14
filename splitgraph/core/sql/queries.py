from typing import Optional, Sequence

from psycopg2.sql import SQL, Composed, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA


def select(
    table: str,
    columns: str = "*",
    where: str = "",
    schema: str = SPLITGRAPH_META_SCHEMA,
    table_args: Optional[str] = None,
) -> Composed:
    """
    A generic SQL SELECT constructor to simplify metadata access queries so that we don't have to repeat the same
    identifiers everywhere.

    :param table: Table to select from.
    :param columns: Columns to select as a string. WARN: concatenated directly without any formatting.
    :param where: If specified, added to the query with a "WHERE" keyword. WARN also concatenated directly.
    :param schema: Defaults to SPLITGRAPH_META_SCHEMA.
    :param table_args: If specified, appends to the FROM clause after the table specification,
        for example, SELECT * FROM "splitgraph_api"."get_images" (%s, %s) ...
    :return: A psycopg2.sql.SQL object with the query.
    """
    query = SQL("SELECT " + columns) + SQL(" FROM {}.{}").format(
        Identifier(schema), Identifier(table)
    )
    if table_args:
        query += SQL(table_args)
    if where:
        query += SQL(" WHERE " + where)
    return query


def insert(table: str, columns: Sequence[str], schema: str = SPLITGRAPH_META_SCHEMA) -> Composed:
    """
    A generic SQL SELECT constructor to simplify metadata access queries so that we don't have to repeat the same
    identifiers everywhere.

    :param table: Table to select from.
    :param columns: Columns to insert as a list of strings.
    :param schema: Schema that contains the table
    :return: A psycopg2.sql.SQL object with the query (parameterized)
    """
    query = SQL("INSERT INTO {}.{}").format(Identifier(schema), Identifier(table))
    query += SQL("(" + ",".join("{}" for _ in columns) + ")").format(*map(Identifier, columns))
    query += SQL("VALUES (" + ",".join("%s" for _ in columns) + ")")
    return query
