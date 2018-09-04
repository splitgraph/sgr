from splitgraph.commands.misc import make_conn
from splitgraph.config import CONFIG
from splitgraph.constants import parse_connection_string, SplitGraphException
from splitgraph.meta_handler import mountpoint_exists


# Parse and set these on import. If we ever need to be able to reread the config on the fly, these have to be
# recalculated.
def _parse_paths_overrides(lookup_path, override_path):
    return ([parse_connection_string(p) for p in lookup_path.split(',')] if lookup_path else [],
            ({p[:p.index(':')]: parse_connection_string(p[p.index(':') + 1:])
              for p in override_path.split(',')} if override_path else {}))


LOOKUP_PATH, LOOKUP_PATH_OVERRIDE = _parse_paths_overrides(CONFIG['SG_REPO_LOOKUP'], CONFIG['SG_REPO_LOOKUP_OVERRIDE'])


def lookup_repo(conn, repo_name, include_local=False):
    """
    Queries the SG drivers on the lookup path to locate one hosting the given driver.
    :param conn: Psycopg connection object
    :param repo_name: Repository name
    :param include_local: If True, also queries the local driver
    :return: A tuple of (server, port, username, password, dbname) to the remote repo or "LOCAL" if the
        local driver has the repository.
    """

    if repo_name in LOOKUP_PATH_OVERRIDE:
        return LOOKUP_PATH_OVERRIDE[repo_name]

    # Currently just check if the schema with that name exists on the remote.
    if include_local and mountpoint_exists(conn, repo_name):
        return "LOCAL"

    for candidate in LOOKUP_PATH:
        remote_conn = make_conn(*candidate)
        if mountpoint_exists(remote_conn, repo_name):
            remote_conn.close()
            return candidate
        remote_conn.close()

    raise SplitGraphException("Unknown repository %s!" % repo_name)
