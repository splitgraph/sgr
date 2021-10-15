"""Routines for managing SQL statements"""
import logging
import re
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union

from psycopg2.sql import SQL, Composed, Identifier
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core.sql._validation import (
    IMPORT_SQL_PERMITTED_STATEMENTS,
    PG_CATALOG_TABLES,
    SPLITFILE_SQL_PERMITTED_STATEMENTS,
)

try:
    import pglast.node
    from pglast import parse_sql
    from pglast.node import Node, Scalar
    from pglast.parser import ParseError
    from pglast.stream import IndentedStream

    _VALIDATION_SUPPORTED = True
except ImportError:
    _VALIDATION_SUPPORTED = False

from splitgraph.exceptions import UnsupportedSQLError

# https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
POSTGRES_MAX_IDENTIFIER = 63


def _validate_range_var(node: "Node") -> None:
    if node.ast_node.schemaname:
        raise UnsupportedSQLError("Table names must not be schema-qualified!")
    if node.ast_node.relname in PG_CATALOG_TABLES:
        raise UnsupportedSQLError("Invalid table name %s!" % node.ast_node.relname)


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
    if isinstance(funcname, pglast.node.List):
        funcname_s = funcname.string_value
    elif funcname.node_tag == "String":
        funcname_s = funcname.val.value
    else:
        raise AssertionError
    if funcname_s.startswith("pg_"):
        raise UnsupportedSQLError("Unsupported function name %s!" % funcname)


# Nodes in this list have extra validators that are supposed to return None or raise an Exception if they
# fail validation.
_SQL_VALIDATORS = {"FuncCall": _validate_funccall}
_IMPORT_SQL_VALIDATORS = {"RangeVar": _validate_range_var, "FuncCall": _validate_funccall}

# Fallback regex to do best-effort SQL rewriting on Windows where pglast doesn't work.
_SCHEMA_RE = re.compile(r'"([\w-]+/)?([\w-]+):([\w-]+)"')


def _validate_node(
    node: Union["Scalar", "Node"],
    permitted_statements: List[str],
    node_validators: Dict[str, Callable],
) -> None:
    if isinstance(node, Scalar):
        return
    node_class = node.node_tag
    if node_class in node_validators:
        node_validators[node_class](node)
    elif node_class.endswith("Stmt") and node_class not in permitted_statements:
        message = "Unsupported statement type %s" % node_class
        if "location" in node.attribute_names and node.ast_node.location:
            message += " near character %d" % node.ast_node.location
        raise UnsupportedSQLError(message + "!")


def _emit_ast(ast: "Node") -> str:
    # required to instantiate all pglast node printers.
    # noinspection PyUnresolvedReferences
    from pglast import printers  # noqa

    stream = IndentedStream()
    return str(stream(ast))


def recover_original_schema_name(sql: str, schema_name: str) -> str:
    """Postgres truncates identifiers to 63 characters at parse time and, as pglast
    uses bits of PG to parse queries, image names like noaa/climate:64_chars_of_hash
    get truncated which can cause ambiguities and issues in provenance. We can't
    get pglast to give us back the full identifier, but we can try and figure out
    what it used to be and patch the AST to have it again.
    """
    if len(schema_name) < POSTGRES_MAX_IDENTIFIER:
        return schema_name

    candidates = list(set(re.findall(r"(" + re.escape(schema_name) + r"[^.\"]*)[.\"]", sql)))
    # Us finding more than one candidate schema is pretty unlikely to happen:
    # we'd have to have a truncated schema name that's 63 characters long
    # (of kind some_namespace/some_repo:abcdef1234567890....)
    # which also somehow features in this query as a non-identifier. Raise an error here if
    # this does happen.
    assert len(candidates) == 1
    return str(candidates[0])


def _rewrite_sql_fallback(sql: str, image_mapper: Callable) -> Tuple[str, str]:
    from splitgraph.core.repository import Repository

    replacements: List[Tuple[str, str, str]] = []

    for namespace, repository, image_hash in _SCHEMA_RE.findall(sql):
        original_text = namespace + repository + ":" + image_hash
        if namespace:
            # Strip the / at the end
            namespace = namespace[:-1]

        temporary_schema, canonical_name = image_mapper(
            Repository(namespace, repository), image_hash
        )
        replacements.append((original_text, temporary_schema, canonical_name))

    canonical_sql = sql
    rewritten_sql = sql

    for original_text, temporary_schema, canonical_name in replacements:
        rewritten_sql = rewritten_sql.replace(original_text, temporary_schema)
        canonical_sql = canonical_sql.replace(original_text, canonical_name)

    return rewritten_sql, canonical_sql


def prepare_splitfile_sql(sql: str, image_mapper: Callable) -> Tuple[str, str]:
    """
    Transform an SQL query to prepare for it to be used in a Splitfile SQL command and validate it.
    The rules are:

      * Only basic DDL (CREATE/ALTER/DROP table) and DML (SELECT/INSERT/UPDATE/DELETE) are permitted.
      * All tables must be either non-schema qualified (the statement is run with `search_path`
      set to the single schema that a Splitgraph image is checked out into) or have schemata of
      format namespace/repository:hash_or_tag. In the second case, the schema is rewritten to point
      at a temporary mount of the Splitgraph image.

    :param sql: SQL query
    :param image_mapper: Takes in an image and gives back the schema it should be rewritten to
        (for the purposes of execution) and the canonical form of the image.
    :return: Transformed form of the SQL with substituted schema shims for Splitfile execution
        and the canonical form (with e.g. tags resolved into at-the-time full image hashes)
    :raises: UnsupportedSQLException if validation failed
    """

    if not _VALIDATION_SUPPORTED:
        logging.warning("SQL validation is unsupported on Windows. SQL will be run unvalidated.")

        return _rewrite_sql_fallback(sql, image_mapper)

    # Avoid circular import
    from splitgraph.core.output import parse_repo_tag_or_hash

    try:
        tree = Node(parse_sql(sql))
    except ParseError as e:
        raise UnsupportedSQLError("Could not parse %s: %s" % (sql, str(e)))

    # List of dict pointers (into parts of the AST) and new schema names we have
    # to rewrite them to. We need to emit two kinds of rewritten SQL: one with schemata
    # replaced with LQ shims (that we send to PostgreSQL for execution) and one with
    # schemata replaced with full image names that we store in provenance_data for reruns etc.
    # On the first pass, we rewrite the parse tree to have the first kind of schemata, then
    # get pglast to serialize it, then use this dictionary to rewrite just the interesting
    # parts of the parse tree (instead of having to re-traverse/re-crawl the tree).
    future_rewrites = []

    for node in tree.traverse():
        _validate_node(
            node,
            permitted_statements=SPLITFILE_SQL_PERMITTED_STATEMENTS,
            node_validators=_SQL_VALIDATORS,
        )

        if not isinstance(node, Node) or node.node_tag != "RangeVar":
            continue

        ast_node = node.ast_node

        if ast_node.relname in PG_CATALOG_TABLES:
            raise UnsupportedSQLError("Invalid table name %s!" % ast_node.relname)
        if not ast_node.schemaname:
            continue

        schema_name = recover_original_schema_name(sql, ast_node.schemaname)

        # If the table name is schema-qualified, rewrite it to talk to a LQ shim
        repo, hash_or_tag = parse_repo_tag_or_hash(schema_name, default="latest")
        temporary_schema, canonical_name = image_mapper(repo, hash_or_tag)

        # We have to access the internal parse tree here to rewrite the schema.
        ast_node.schemaname = temporary_schema
        future_rewrites.append((ast_node, canonical_name))

    rewritten_sql = _emit_ast(tree)

    for tree_subset, canonical_name in future_rewrites:
        tree_subset.schemaname = canonical_name

    canonical_sql = _emit_ast(tree)
    return rewritten_sql, canonical_sql


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
            node,
            permitted_statements=IMPORT_SQL_PERMITTED_STATEMENTS,
            node_validators=_IMPORT_SQL_VALIDATORS,
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
