"""
Routines for managing Splitgraph engines, including looking up repositories and managing objects.
"""
import logging

from psycopg2.sql import SQL, Identifier
from splitgraph.config import SPLITGRAPH_META_SCHEMA, CONFIG
from splitgraph.engine import ResultShape, get_engine
from splitgraph.exceptions import SplitGraphException

from ._common import select, ensure_metadata_schema


def _parse_paths_overrides(lookup_path, override_path):
    return (lookup_path.split(',') if lookup_path else [],
            {r[:r.index(':')]: r[r.index(':') + 1:]
             for r in override_path.split(',')} if override_path else {})


# Parse and set these on import. If we ever need to be able to reread the config on the fly, these have to be
# recalculated.
_LOOKUP_PATH, _LOOKUP_PATH_OVERRIDE = \
    _parse_paths_overrides(CONFIG['SG_REPO_LOOKUP'], CONFIG['SG_REPO_LOOKUP_OVERRIDE'])


def init_engine():  # pragma: no cover
    # Method exercised in test_commandline.test_init_new_db but in
    # an external process
    """
    Initializes the engine by:

        * performing any required engine-custom initialization
        * creating the metadata tables

    """
    # Initialize the engine
    engine = get_engine()
    engine.initialize()

    # Create splitgraph_meta
    logging.info("Ensuring metadata schema %s exists...", SPLITGRAPH_META_SCHEMA)
    ensure_metadata_schema(engine)
    engine.commit()


def repository_exists(repository):
    """
    Checks if a repository exists on the engine.

    :param repository: Repository object
    """
    return repository.engine.run_sql(SQL("SELECT 1 FROM {}.images WHERE namespace = %s AND repository = %s")
                                     .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                                     (repository.namespace, repository.repository),
                                     return_shape=ResultShape.ONE_ONE) is not None or \
           repository.engine.run_sql(SQL("SELECT 1 FROM {}.tags WHERE namespace = %s AND repository = %s")
                                     .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                                     (repository.namespace, repository.repository),
                                     return_shape=ResultShape.ONE_ONE) is not None


def lookup_repository(name, include_local=False):
    """
    Queries the SG drivers on the lookup path to locate one hosting the given repository.

    :param name: Repository name
    :param include_local: If True, also queries the local engine

    :return: Local or remote Repository object
    """
    from splitgraph.core.repository import Repository
    template = Repository.from_schema(name)

    if name in _LOOKUP_PATH_OVERRIDE:
        return Repository(template.namespace, template.repository,
                          get_engine(_LOOKUP_PATH_OVERRIDE[name]))

    # Currently just check if the schema with that name exists on the remote.
    if include_local and repository_exists(template):
        return template

    for engine in _LOOKUP_PATH:
        candidate = Repository(template.namespace, template.repository, get_engine(engine))
        if repository_exists(candidate):
            return candidate
        candidate.engine.close()

    raise SplitGraphException("Unknown repository %s!" % name)


def get_current_repositories(engine):
    """
    Lists all repositories currently in the engine.

    :param engine: Engine
    :return: List of (Repository object, current HEAD image)
    """
    ensure_metadata_schema(engine)
    from splitgraph.core.repository import Repository

    all_repositories = [Repository(n, r, engine) for n, r in
                        engine.run_sql(select("images", "DISTINCT namespace,repository"))]
    return [(r, r.head) for r in all_repositories]
