"""
Routines for managing Splitgraph engines, including looking up repositories and managing objects.
"""
import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from psycopg2.sql import SQL, Identifier
from splitgraph.config import CONFIG, SPLITGRAPH_API_SCHEMA, get_singleton
from splitgraph.engine import ResultShape, get_engine
from splitgraph.exceptions import RepositoryNotFoundError

from .sql import select

if TYPE_CHECKING:
    from splitgraph.core.image import Image
    from splitgraph.core.repository import Repository
    from splitgraph.engine.postgres.engine import PostgresEngine


def _parse_paths_overrides(
    lookup_path: str, override_path: str
) -> Tuple[List[str], Dict[str, str]]:
    return (
        lookup_path.split(",") if lookup_path else [],
        {r[: r.index(":")]: r[r.index(":") + 1 :] for r in override_path.split(",")}
        if override_path
        else {},
    )


# Parse and set these on import. If we ever need to be able to reread the config on the fly, these have to be
# recalculated.
_LOOKUP_PATH, _LOOKUP_PATH_OVERRIDE = _parse_paths_overrides(
    get_singleton(CONFIG, "SG_REPO_LOOKUP"), get_singleton(CONFIG, "SG_REPO_LOOKUP_OVERRIDE")
)


def init_engine(skip_object_handling: bool = False) -> None:  # pragma: no cover
    # Method exercised in test_commandline.test_init_new_db but in
    # an external process
    """
    Initializes the engine by:

        * performing any required engine-custom initialization
        * creating the metadata tables

    :param skip_object_handling: If True, skips installing routines related to
        object handling and checkouts (like audit triggers and CStore management).
    """
    # Initialize the engine
    engine = get_engine()
    engine.initialize(skip_object_handling=skip_object_handling)
    engine.commit()
    logging.info("Engine %r initialized.", engine)


def repository_exists(repository: "Repository") -> bool:
    """
    Checks if a repository exists on the engine.

    :param repository: Repository object
    """
    return (
        repository.engine.run_sql(
            SQL("SELECT 1 FROM {}.get_images(%s,%s)").format(Identifier(SPLITGRAPH_API_SCHEMA)),
            (repository.namespace, repository.repository),
            return_shape=ResultShape.ONE_ONE,
        )
        is not None
    )


def lookup_repository(name: str, include_local: bool = False) -> "Repository":
    """
    Queries the SG engines on the lookup path to locate one hosting the given repository.

    :param name: Repository name
    :param include_local: If True, also queries the local engine

    :return: Local or remote Repository object
    """
    from splitgraph.core.repository import Repository

    template = Repository.from_schema(name)

    if name in _LOOKUP_PATH_OVERRIDE:
        return Repository(
            template.namespace, template.repository, get_engine(_LOOKUP_PATH_OVERRIDE[name])
        )

    # Currently just check if the schema with that name exists on the remote.
    if include_local and repository_exists(template):
        return template

    for engine in _LOOKUP_PATH:
        candidate = Repository(template.namespace, template.repository, get_engine(engine))
        if repository_exists(candidate):
            return candidate
        candidate.engine.close()

    raise RepositoryNotFoundError("Unknown repository %s!" % name)


def get_current_repositories(
    engine: "PostgresEngine",
) -> List[Tuple["Repository", Optional["Image"]]]:
    """
    Lists all repositories currently in the engine.

    :param engine: Engine
    :return: List of (Repository object, current HEAD image)
    """
    from splitgraph.core.repository import Repository

    all_repositories = [
        Repository(n, r, engine)
        for n, r in engine.run_sql(select("images", "DISTINCT namespace,repository"))
    ]
    return [(r, r.head) for r in all_repositories]
