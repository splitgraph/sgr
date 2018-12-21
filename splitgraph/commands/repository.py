"""
Functions to manage Splitgraph repositories
"""

from datetime import datetime

from psycopg2.sql import SQL, Identifier

from splitgraph.config import CONFIG
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.engine import ResultShape, get_engine, switch_engine
from splitgraph.exceptions import SplitGraphException
from .._data.common import ensure_metadata_schema, insert, select


def _parse_paths_overrides(lookup_path, override_path):
    return (lookup_path.split(',') if lookup_path else [],
            {r[:r.index(':')]: r[r.index(':') + 1:]
             for r in override_path.split(',')} if override_path else {})


# Parse and set these on import. If we ever need to be able to reread the config on the fly, these have to be
# recalculated.
_LOOKUP_PATH, _LOOKUP_PATH_OVERRIDE = \
    _parse_paths_overrides(CONFIG['SG_REPO_LOOKUP'], CONFIG['SG_REPO_LOOKUP_OVERRIDE'])


def repository_exists(repository):
    """
    Checks if a repository exists on the engine. Can be used with `override_engine_connection`

    :param repository: Repository object
    """
    return get_engine().run_sql(SQL("SELECT 1 FROM {}.images WHERE namespace = %s AND repository = %s")
                                .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                                (repository.namespace, repository.repository),
                                return_shape=ResultShape.ONE_ONE) is not None


def register_repository(repository, initial_image):
    """
    Registers a new repository on the engine. Internal function, use `splitgraph.init` instead.

    :param repository: Repository object
    :param initial_image: Hash of the initial image
    """
    engine = get_engine()
    engine.run_sql(insert("images", ("image_hash", "namespace", "repository", "parent_id", "created")),
                   (initial_image, repository.namespace, repository.repository, None, datetime.now()))
    # Strictly speaking this is redundant since the checkout (of the "HEAD" commit) updates the tag table.
    engine.run_sql(insert("tags", ("namespace", "repository", "image_hash", "tag")),
                   (repository.namespace, repository.repository, initial_image, "HEAD"))


def unregister_repository(repository, is_remote=False):
    """
    Deregisters the repository. Internal function, use splitgraph.rm to delete a repository.

    :param repository: Repository object
    :param is_remote: Specifies whether the engine is a remote that doesn't have the "upstream" table.
    """
    engine = get_engine()
    meta_tables = ["tables", "tags", "images"]
    if not is_remote:
        meta_tables.append("upstream")
    for meta_table in meta_tables:
        engine.run_sql(SQL("DELETE FROM {}.{} WHERE namespace = %s AND repository = %s")
                       .format(Identifier(SPLITGRAPH_META_SCHEMA),
                               Identifier(meta_table)),
                       (repository.namespace, repository.repository))


def get_current_repositories():
    """
    Lists all repositories currently in the engine.

    :return: List of (Repository object, current HEAD image)
    """
    ensure_metadata_schema()
    from splitgraph.core.repository import Repository
    return [(Repository(n, r), i) for n, r, i in
            get_engine().run_sql(select("tags", "namespace, repository, image_hash", "tag = 'HEAD'"))]


def lookup_repo(repo_name, include_local=False):
    """
    Queries the SG drivers on the lookup path to locate one hosting the given engine.

    :param repo_name: Repository name
    :param include_local: If True, also queries the local engine

    :return: The name of the remote engine that has the repository (as specified in the config)
        or "LOCAL" if the local engine has the repository.
    """

    if repo_name in _LOOKUP_PATH_OVERRIDE:
        return _LOOKUP_PATH_OVERRIDE[repo_name]

    # Currently just check if the schema with that name exists on the remote.
    if include_local and repository_exists(repo_name):
        return "LOCAL"

    for candidate in _LOOKUP_PATH:
        with switch_engine(candidate):
            if repository_exists(repo_name):
                get_engine().close()
                return candidate
            get_engine().close()

    raise SplitGraphException("Unknown repository %s!" % repo_name.to_schema())
