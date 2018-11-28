from splitgraph.config import CONFIG
from splitgraph.connection import override_driver_connection, make_conn
from splitgraph.exceptions import SplitGraphException
from splitgraph.meta_handler.misc import repository_exists


# Parse and set these on import. If we ever need to be able to reread the config on the fly, these have to be
# recalculated.
def _parse_paths_overrides(lookup_path, override_path):
    return ([get_remote_connection_params(r) for r in lookup_path.split(',')] if lookup_path else [],
            ({r[:r.index(':')]: get_remote_connection_params(r[r.index(':') + 1:])
              for r in override_path.split(',')} if override_path else {}))


def get_remote_connection_params(remote_name):
    pdict = CONFIG['remotes'][remote_name]
    return (pdict['SG_DRIVER_HOST'], int(pdict['SG_DRIVER_PORT']), pdict['SG_DRIVER_USER'],
            pdict['SG_DRIVER_PWD'], pdict['SG_DRIVER_DB_NAME'])


LOOKUP_PATH, LOOKUP_PATH_OVERRIDE = _parse_paths_overrides(CONFIG['SG_REPO_LOOKUP'], CONFIG['SG_REPO_LOOKUP_OVERRIDE'])


def lookup_repo(repo_name, include_local=False):
    """
    Queries the SG drivers on the lookup path to locate one hosting the given driver.
    :param repo_name: Repository name
    :param include_local: If True, also queries the local driver

    :return: A tuple of (server, port, username, password, dbname) to the remote repo or "LOCAL" if the
        local driver has the repository.
    """

    if repo_name in LOOKUP_PATH_OVERRIDE:
        return LOOKUP_PATH_OVERRIDE[repo_name]

    # Currently just check if the schema with that name exists on the remote.
    if include_local and repository_exists(repo_name):
        return "LOCAL"

    for candidate in LOOKUP_PATH:
        remote_conn = make_conn(*candidate)
        with override_driver_connection(remote_conn):
            if repository_exists(repo_name):
                remote_conn.close()
                return candidate
            remote_conn.close()

    raise SplitGraphException("Unknown repository %s!" % repo_name.to_schema())
