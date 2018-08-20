import os
from random import getrandbits

PG_HOST = os.getenv('PGHOST', 'localhost')
PG_PORT = os.getenv('PGPORT', '5432')
PG_DB = os.getenv('PGDATABASE', 'cachedb')
PG_USER = os.getenv('PGUSER', 'clientuser')
PG_PWD = os.getenv('PGPASSWORD', 'supersecure')
POSTGRES_CONNECTION = "host=%s port=%s dbname=%s user=%s password=%s" % (PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PWD)
SPLITGRAPH_META_SCHEMA = "splitgraph_meta"


class SplitGraphException(Exception):
    pass


def _log(text):
    print(text)


def get_random_object_id():
    # Assign each table a random ID that it will be stored as.
    # Note that postgres limits table names to 63 characters, so
    # the IDs shall be 248-bit strings, hex-encoded, + a letter prefix since Postgres
    # doesn't seem to support table names starting with a digit?
    return "o%0.2x" % getrandbits(248)
