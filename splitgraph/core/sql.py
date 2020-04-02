"""Routines for managing SQL statements"""
import logging
from typing import Callable, Dict, List, Union, Optional, Sequence

from pglast.printer import RawStream, IndentedStream
from psycopg2.sql import Composed, SQL, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA

try:
    from pglast import parse_sql
    from pglast.node import Node, Scalar
    from pglast.parser import ParseError

    _VALIDATION_SUPPORTED = True
except ImportError:
    _VALIDATION_SUPPORTED = False

from splitgraph.exceptions import UnsupportedSQLError


def _validate_range_var(node: "Node") -> None:
    if "schemaname" in node.attribute_names:
        raise UnsupportedSQLError("Table names must not be schema-qualified!")


def _validate_funccall(node: "Node"):
    # We can't ban all function calls (there are some useful ones like
    # to_date() etc) but we can't allow all of them either (some pg system
    # calls like pg_relation_filepath etc) aren't appropriate here but are available
    # to all users by default -- so as a preliminary defense we stop all functions that
    # begin with pg_.
    funcname = node.funcname
    if len(funcname) != 1:
        # e.g. pg_catalog.substring
        funcname = funcname[1]
    if funcname.string_value.startswith("pg_"):
        raise UnsupportedSQLError("Unsupported function name %s!" % funcname)


# Whitelist of permitted AST nodes. When crawling the parse tree, a node not in this list fails validation. If a node
# is in this list, the crawler continues down the tree.
_IMPORT_SQL_PERMITTED_NODES = [
    "RawStmt",
    "SelectStmt",
    "ResTarget",
    "ColumnRef",
    "A_Star",
    "String",
    "A_Expr",
    "A_Const",
    "Integer",
    "JoinExpr",
    "SortBy",
    "NullTest",
    "BoolExpr",
    "CoalesceExpr",
    "RangeFunction",
    "TypeCast",
    "TypeName",
    "SubLink",
    "WithClause",
    "CommonTableExpr",
    "A_ArrayExpr",
    "Float",
    "CaseExpr",
    "CaseWhen",
    "Alias",
]

_SPLITFILE_SQL_PERMITTED_NODES = _IMPORT_SQL_PERMITTED_NODES + [
    "InsertStmt",
    "UpdateStmt",
    "DeleteStmt",
    "CreateStmt",
    "CreateTableAsStmt",
    "IntoClause",
    "AlterTableStmt",
    "AlterTableCmd",
    "DropStmt",
    "ColumnDef",
    "Constraint",
]

# Nodes in this list have extra validators that are supposed to return None or raise an Exception if they
# fail validation.
_SQL_VALIDATORS = {"RangeVar": _validate_range_var, "FuncCall": _validate_funccall}


def _validate_node(
    node: Union["Scalar", "Node"], permitted_nodes: List[str], node_validators: Dict[str, Callable]
) -> None:
    if isinstance(node, Scalar):
        return
    node_class = node.node_tag
    if node_class in node_validators:
        node_validators[node_class](node)
    elif node_class not in permitted_nodes:
        message = "Unsupported statement type %s" % node_class
        if isinstance(node["location"], Scalar):
            message += " near character %d" % node["location"].value
        raise UnsupportedSQLError(message + "!")


def _emit_ast(ast: "Node") -> str:
    # required to instantiate all pglast node printers.
    # noinspection PyUnresolvedReferences
    from pglast import printers  # noqa

    stream = IndentedStream()
    return stream(ast)


def validate_splitfile_sql(sql: str) -> str:
    """
    Check an SQL query to see if it can be safely used in a Splitfile SQL command. The rules for usage are:

      * Only basic DDL (CREATE/ALTER/DROP table) and DML (SELECT/INSERT/UPDATE/DELETE) are permitted.
      * All tables must be non-schema-qualified (the statement is run with `search_path` set to the single
        schema that a Splitgraph image is checked out into).
      * Function invocations are forbidden.

    :param sql: SQL query
    :return: Canonical (reformatted) form of the SQL.
    :raises: UnsupportedSQLException if validation failed
    """
    if not _VALIDATION_SUPPORTED:
        logging.warning("SQL validation is unsupported on Windows. SQL will be run unvalidated.")
        return sql

    try:
        tree = Node(parse_sql(sql))
    except ParseError as e:
        raise UnsupportedSQLError("Could not parse %s: %s" % (sql, str(e)))
    for node in tree.traverse():
        _validate_node(
            node, permitted_nodes=_SPLITFILE_SQL_PERMITTED_NODES, node_validators=_SQL_VALIDATORS
        )
    return _emit_ast(tree)


def validate_import_sql(sql: str) -> str:
    """
    Check an SQL query to see if it can be safely used in an IMPORT statement
    (e.g. `FROM noaa/climate:latest IMPORT {SELECT * FROM rainfall WHERE state = 'AZ'} AS rainfall`.
    In this case, only a single SELECT statement is supported.

    :param sql: SQL query
    :return: Canonical (formatted) form of the SQL statement
    :raises: UnsupportedSQLException if validation failed
    """
    if not _VALIDATION_SUPPORTED:
        logging.warning("SQL validation is unsupported on Windows. SQL will be run unvalidated.")
        return sql

    try:
        tree = Node(parse_sql(sql))
    except ParseError as e:
        raise UnsupportedSQLError("Could not parse %s: %s" % (sql, str(e)))
    if len(tree) != 1:
        raise UnsupportedSQLError("The query is supposed to consist of only one SELECT statement!")

    for node in tree.traverse():
        _validate_node(
            node, permitted_nodes=_IMPORT_SQL_PERMITTED_NODES, node_validators=_SQL_VALIDATORS
        )
    return _emit_ast(tree)


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
