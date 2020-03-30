import logging
import re
from collections import defaultdict
from datetime import datetime
from typing import Optional, Tuple, cast, TYPE_CHECKING, List, TypeVar, Dict, DefaultDict

from psycopg2.sql import SQL, Identifier

from splitgraph.core.sql import select, insert
from splitgraph.engine import ResultShape

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PsycopgEngine

T = TypeVar("T")


def _bfs(adjacency: Dict[T, List[T]], start: T, end: T) -> List[T]:
    queue = [start]
    visited = {start}
    parents: Dict[T, T] = {}

    while queue:
        vertex = queue[0]
        queue = queue[1:]

        if vertex == end:
            path = [end]
            while vertex != start:
                vertex = parents[vertex]
                path.append(vertex)
            path.reverse()
            return path
        for v in adjacency[vertex]:
            if v in visited:
                continue
            visited.add(v)
            parents[v] = vertex
            queue.append(v)

    raise ValueError("No path found between %s and %s" % (start, end))


def make_file_list(schema_name: str, migration_path: List[Optional[str]]):
    """Construct a list of file names from history of versions and schema name"""
    result = []
    for v1, v2 in zip(migration_path, migration_path[1:]):
        if v1 is None:
            name = "%s--%s.sql" % (schema_name, v2)
        else:
            name = "%s--%s--%s.sql" % (schema_name, v1, v2)
        result.append(name)
    return result


def get_installed_version(
    engine: "PsycopgEngine", schema_name: str, version_table: str = "version"
) -> Optional[Tuple[str, datetime]]:
    if not engine.table_exists(schema_name, version_table):
        return None
    return cast(
        Optional[Tuple[str, datetime]],
        engine.run_sql(
            select(version_table, "version,installed", schema=schema_name)
            + SQL("ORDER BY installed DESC LIMIT 1"),
            return_shape=ResultShape.ONE_MANY,
        ),
    )


def _create_version_tracking_schema(
    engine, schema_name: str, version_table: str = "version"
) -> None:
    engine.run_sql(
        SQL(
            """CREATE TABLE {0}.{1} (version VARCHAR NOT NULL,
            installed TIMESTAMP);"""
        ).format(Identifier(schema_name), Identifier(version_table))
    )


def set_installed_version(
    engine: "PsycopgEngine", schema_name: str, version: str, version_table: str = "version"
):
    if not engine.table_exists(schema_name, version_table):
        _create_version_tracking_schema(engine, schema_name, version_table)
    engine.run_sql(
        insert(version_table, ["version", "installed"], schema=schema_name),
        (version, datetime.utcnow()),
    )


def _get_version_tuples(filenames: List[str]) -> List[Tuple[Optional[str], str]]:
    version_re = re.compile(r"\w+--([\w.]+)(--[\w.]+)?\.sql")

    result: List[Tuple[Optional[str], str]] = []
    for name in filenames:
        match = version_re.match(name)
        if not match:
            raise ValueError("Invalid file name %s" % name)
        if match.groups()[1] is None:
            result.append((None, match.groups()[0]))
        else:
            result.append((match.groups()[0], match.groups()[1].lstrip("-")))

    return result


def source_files_to_apply(
    engine: "PsycopgEngine",
    schema_name: str,
    schema_files: List[str],
    version_table: str = "version",
    static: bool = False,
    target_version: Optional[str] = None,
) -> Tuple[List[str], str]:
    """ Get the ordered list of .sql files to apply to the database"""
    version_tuples = _get_version_tuples(schema_files)
    target_version = target_version or max([v[1] for v in version_tuples])
    if static:
        # For schemata like splitgraph_api which we want to apply in any
        # case, bypassing the upgrade mechanism, we just run the latest
        # set of sources that this schema needs.
        path = [None, target_version]
    else:
        current = get_installed_version(engine, schema_name, version_table)

        current_version: Optional[str]
        if current:
            current_version, current_install_date = current
            logging.info(
                "Schema %s already exists, version %s, installed on %s",
                schema_name,
                current_version,
                current_install_date,
            )
        else:
            current_version = None

        if current_version == target_version:
            return [], target_version

        # Calculate migration path
        adjacency: DefaultDict[Optional[str], List[Optional[str]]] = defaultdict(list)
        for u, v in version_tuples:
            adjacency[u].append(v)
        path = _bfs(adjacency, start=current_version, end=target_version)

    return make_file_list(schema_name, path), target_version
