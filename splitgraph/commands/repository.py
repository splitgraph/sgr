"""
Functions to manage Splitgraph repositories
"""

from collections.__init__ import namedtuple
from datetime import datetime

from psycopg2.sql import SQL, Identifier

from splitgraph.config import CONFIG
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.connection import make_conn, override_driver_connection, get_connection
from splitgraph.exceptions import SplitGraphException
from .._data.common import ensure_metadata_schema, insert, select
from .._data.objects import register_object, register_table


def get_remote_connection_params(remote_name):
    """
    Gets connection parameters for a Splitgraph remote.
    :param remote_name: Name of the remote. Must be specified in the config file.
    :return: A tuple of (hostname, port, username, password, database)
    """
    pdict = CONFIG['remotes'][remote_name]
    return (pdict['SG_DRIVER_HOST'], int(pdict['SG_DRIVER_PORT']), pdict['SG_DRIVER_USER'],
            pdict['SG_DRIVER_PWD'], pdict['SG_DRIVER_DB_NAME'])


def _parse_paths_overrides(lookup_path, override_path):
    return ([get_remote_connection_params(r) for r in lookup_path.split(',')] if lookup_path else [],
            ({r[:r.index(':')]: get_remote_connection_params(r[r.index(':') + 1:])
              for r in override_path.split(',')} if override_path else {}))


# Parse and set these on import. If we ever need to be able to reread the config on the fly, these have to be
# recalculated.
_LOOKUP_PATH, _LOOKUP_PATH_OVERRIDE = \
    _parse_paths_overrides(CONFIG['SG_REPO_LOOKUP'], CONFIG['SG_REPO_LOOKUP_OVERRIDE'])


class Repository(namedtuple('Repository', ['namespace', 'repository'])):
    """
    A repository object that encapsulates the namespace and the actual repository name.
    """

    def to_schema(self):
        """Converts the object to a Postgres schema."""
        return self.repository if not self.namespace else self.namespace + '/' + self.repository

    __repr__ = to_schema
    __str__ = to_schema


def to_repository(schema):
    """Converts a Postgres schema name of the format `namespace/repository` to a Splitgraph repository object."""
    if '/' in schema:
        namespace, repository = schema.split('/')
        return Repository(namespace, repository)
    return Repository('', schema)


def repository_exists(repository):
    """
    Checks if a repository exists on the driver. Can be used with `override_driver_connection`
    :param repository: Repository object
    """
    with get_connection().cursor() as cur:
        cur.execute(SQL("SELECT 1 FROM {}.images WHERE namespace = %s AND repository = %s")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                    (repository.namespace, repository.repository))
        return cur.fetchone() is not None


def register_repository(repository, initial_image, tables, table_object_ids):
    """
    Registers a new repository on the driver. Internal function, use `splitgraph.init` instead.
    :param repository: Repository object
    :param initial_image: Hash of the initial image
    :param tables: Table names in the initial image
    :param table_object_ids: Object IDs that the initial tables map to.
    """
    with get_connection().cursor() as cur:
        cur.execute(insert("images", ("image_hash", "namespace", "repository", "parent_id", "created")),
                    (initial_image, repository.namespace, repository.repository, None, datetime.now()))
        # Strictly speaking this is redundant since the checkout (of the "HEAD" commit) updates the tag table.
        cur.execute(insert("tags", ("namespace", "repository", "image_hash", "tag")),
                    (repository.namespace, repository.repository, initial_image, "HEAD"))
        for t, ti in zip(tables, table_object_ids):
            # Register the tables and the object IDs they were stored under.
            # They're obviously stored as snaps since there's nothing to diff to...
            register_object(ti, 'SNAP', repository.namespace, None)
            register_table(repository, t, initial_image, ti)


def unregister_repository(repository, is_remote=False):
    """
    Deregisters the repository. Internal function/
    :param repository: Repository object
    :param is_remote: Specifies whether the driver is a remote that doesn't have the "remotes" table.
    """
    with get_connection().cursor() as cur:
        meta_tables = ["tables", "tags", "images"]
        if not is_remote:
            meta_tables.append("remotes")
        for meta_table in meta_tables:
            cur.execute(SQL("DELETE FROM {}.{} WHERE namespace = %s AND repository = %s")
                        .format(Identifier(SPLITGRAPH_META_SCHEMA),
                                Identifier(meta_table)),
                        (repository.namespace, repository.repository))


def get_current_repositories():
    """
    Lists all repositories currently in the driver.
    :return: List of (Repository object, current HEAD image)
    """
    ensure_metadata_schema()
    with get_connection().cursor() as cur:
        cur.execute(select("tags", "namespace, repository, image_hash", "tag = 'HEAD'"))
        return [(Repository(n, r), i) for n, r, i in cur.fetchall()]


# TODO these remotes are different from the remote drivers -- here remotes map a locally cloned repository
# to the remote connection string + remote repository so that we can do "sgr push/pull" without any context.

def get_remote_for(repository, remote_name='origin'):
    """
    Gets the current remote (connection string and repository) that a local repository tracks
    :param repository: Local repository
    :param remote_name: Alias for the remote, default 'origin'
    :return: Tuple of (connection string, remote Repository object)
    """
    with get_connection().cursor() as cur:
        cur.execute(select("remotes", "remote_conn_string, remote_namespace, remote_repository",
                           "namespace = %s AND repository = %s AND remote_name = %s"),
                    (repository.namespace, repository.repository, remote_name))
        result = cur.fetchone()
        if result is None:
            return result
        cs, ns, re = result
        return cs, Repository(ns, re)


def add_remote(repository, remote_conn, remote_repository, remote_name='origin'):
    """
    Adds a remote that a local repository tracks.
    :param repository: Local repository
    :param remote_conn: Remote connection string
    :param remote_repository: Remote Repository object
    :param remote_name: Alias to give this remote
    """
    with get_connection().cursor() as cur:
        cur.execute(insert("remotes", ("namespace", "repository", "remote_name", "remote_conn_string",
                                       "remote_namespace", "remote_repository")),
                    (repository.namespace, repository.repository, remote_name,
                     remote_conn, remote_repository.namespace, remote_repository.repository))


def lookup_repo(repo_name, include_local=False):
    """
    Queries the SG drivers on the lookup path to locate one hosting the given driver.
    :param repo_name: Repository name
    :param include_local: If True, also queries the local driver

    :return: A tuple of (server, port, username, password, dbname) to the remote repo or "LOCAL" if the
        local driver has the repository.
    """

    if repo_name in _LOOKUP_PATH_OVERRIDE:
        return _LOOKUP_PATH_OVERRIDE[repo_name]

    # Currently just check if the schema with that name exists on the remote.
    if include_local and repository_exists(repo_name):
        return "LOCAL"

    for candidate in _LOOKUP_PATH:
        remote_conn = make_conn(*candidate)
        with override_driver_connection(remote_conn):
            if repository_exists(repo_name):
                remote_conn.close()
                return candidate
            remote_conn.close()

    raise SplitGraphException("Unknown repository %s!" % repo_name.to_schema())
