"""
Routines for managing Splitgraph engines, including looking up repositories and managing objects.
"""
import logging

from psycopg2.sql import SQL, Identifier

from splitgraph import get_engine, SplitGraphException
from splitgraph._data.common import META_TABLES, ensure_metadata_schema, select
from splitgraph._data.objects import get_object_meta
from splitgraph.config import SPLITGRAPH_META_SCHEMA, CONFIG
from splitgraph.engine import ResultShape


def _parse_paths_overrides(lookup_path, override_path):
    return (lookup_path.split(',') if lookup_path else [],
            {r[:r.index(':')]: r[r.index(':') + 1:]
             for r in override_path.split(',')} if override_path else {})


# Parse and set these on import. If we ever need to be able to reread the config on the fly, these have to be
# recalculated.
_LOOKUP_PATH, _LOOKUP_PATH_OVERRIDE = \
    _parse_paths_overrides(CONFIG['SG_REPO_LOOKUP'], CONFIG['SG_REPO_LOOKUP_OVERRIDE'])


def cleanup_objects(include_external=False):
    """
    Deletes all local objects not required by any current mountpoint, including their dependencies, their remote
    locations and their cached local copies.

    :param include_external: If True, deletes all external objects cached locally and redownloads them when they're
        needed.
    """
    # First, get a list of all objects required by a table.
    engine = get_engine()

    primary_objects = set(engine.run_sql(
        SQL("SELECT DISTINCT (object_id) FROM {}.tables").format(Identifier(SPLITGRAPH_META_SCHEMA)),
        return_shape=ResultShape.MANY_ONE))

    # Expand that since each object might have a parent it depends on.
    if primary_objects:
        while True:
            new_parents = set(parent_id for _, _, parent_id, _ in get_object_meta(list(primary_objects))
                              if parent_id not in primary_objects and parent_id is not None)
            if not new_parents:
                break
            else:
                primary_objects.update(new_parents)

    # Go through the tables that aren't mountpoint-dependent and delete entries there.
    for table_name in ['objects', 'object_locations']:
        query = SQL("DELETE FROM {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(table_name))
        if primary_objects:
            query += SQL(" WHERE object_id NOT IN (" + ','.join('%s' for _ in range(len(primary_objects))) + ")")
        engine.run_sql(query, list(primary_objects))

    # Go through the physical objects and delete them as well
    # This is slightly dirty, but since the info about the objects was deleted on rm, we just say that
    # anything in splitgraph_meta that's not a system table is fair game.
    tables_in_meta = {c for c in engine.get_all_tables(SPLITGRAPH_META_SCHEMA) if c not in META_TABLES}

    to_delete = tables_in_meta.difference(primary_objects)

    # All objects in `object_locations` are assumed to exist externally (so we can redownload them if need be).
    # This can be improved on by, on materialization, downloading all SNAPs directly into the target schema and
    # applying the DIFFs to it (instead of downloading them into a staging area), but that requires us to change
    # the object downloader interface.
    if include_external:
        to_delete = to_delete.union(set(
            engine.run_sql(SQL("SELECT object_id FROM {}.object_locations")
                           .format(Identifier(SPLITGRAPH_META_SCHEMA)), return_shape=ResultShape.MANY_ONE)))

    delete_objects(to_delete)
    return to_delete


def delete_objects(objects):
    """
    Deletes objects from the Splitgraph cache

    :param objects: A sequence of objects to be deleted
    """
    engine = get_engine()
    for o in objects:
        engine.delete_table(SPLITGRAPH_META_SCHEMA, o)


def init_engine():  # pragma: no cover
    # Method exercised in test_commandline.test_init_new_db but in
    # an external process
    """
    Initializes the engine by:

        * performing any required engine-custom initialization
        * creating the metadata tables

    """
    # Initialize the engine
    get_engine().initialize()

    # Create splitgraph_meta
    logging.info("Ensuring metadata schema %s exists...", SPLITGRAPH_META_SCHEMA)
    ensure_metadata_schema()


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


def lookup_repo(repo_name, include_local=False):
    """
    Queries the SG drivers on the lookup path to locate one hosting the given repository.

    :param repo_name: Repository name
    :param include_local: If True, also queries the local engine

    :return: Local or remote Repository object
    """
    from splitgraph.core.repository import to_repository
    result = to_repository(repo_name)

    if repo_name in _LOOKUP_PATH_OVERRIDE:
        result.engine = get_engine(_LOOKUP_PATH_OVERRIDE[repo_name])
        return result

    # Currently just check if the schema with that name exists on the remote.
    if include_local and repository_exists(result):
        return result

    for candidate in _LOOKUP_PATH:
        result.engine = get_engine(candidate)
        if repository_exists(result):
            return result
        result.engine.close()

    raise SplitGraphException("Unknown repository %s!" % repo_name.to_schema())


def get_current_repositories():
    """
    Lists all repositories currently in the engine.

    :return: List of (Repository object, current HEAD image)
    """
    ensure_metadata_schema()
    from splitgraph.core.repository import Repository
    engine = get_engine()

    all_repositories = [Repository(n, r, engine) for n, r in
                        get_engine().run_sql(select("images", "DISTINCT namespace,repository"))]
    return [(r, r.get_head(raise_on_none=False)) for r in all_repositories]
