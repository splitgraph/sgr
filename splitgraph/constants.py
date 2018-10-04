import logging
import re
from random import getrandbits

from splitgraph.config import CONFIG

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

PG_HOST = CONFIG["SG_DRIVER_HOST"]
PG_PORT = CONFIG["SG_DRIVER_PORT"]
PG_DB = CONFIG["SG_DRIVER_DB_NAME"]
PG_USER = CONFIG["SG_DRIVER_USER"]
PG_PWD = CONFIG["SG_DRIVER_PWD"]
POSTGRES_CONNECTION = CONFIG["SG_DRIVER_CONNECTION_STRING"]
SPLITGRAPH_META_SCHEMA = CONFIG["SG_META_SCHEMA"]
REGISTRY_META_SCHEMA = "registry_meta"


class SplitGraphException(Exception):
    pass


def get_random_object_id():
    """Assign each table a random ID that it will be stored as. Note that postgres limits table names to 63 characters,
    so the IDs shall be 248-bit strings, hex-encoded, + a letter prefix since Postgres doesn't seem to support table
    names starting with a digit."""
    return "o%0.2x" % getrandbits(248)


def parse_connection_string(conn_string):
    """
    :return: a tuple (server, port, username, password, dbname)
    """
    match = re.match(r'(\S+):(\S+)@(.+):(\d+)/(\S+)', conn_string)
    return match.group(3), int(match.group(4)), match.group(1), match.group(2), match.group(5)


def serialize_connection_string(server, port, username, password, dbname):
    return '%s:%s@%s:%s/%s' % (username, password, server, port, dbname)


class Color:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'
