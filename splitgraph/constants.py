import logging
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


class SplitGraphException(Exception):
    pass


def get_random_object_id():
    """Assign each table a random ID that it will be stored as. Note that postgres limits table names to 63 characters,
    so the IDs shall be 248-bit strings, hex-encoded, + a letter prefix since Postgres doesn't seem to support table
    names starting with a digit."""
    return "o%0.2x" % getrandbits(248)
