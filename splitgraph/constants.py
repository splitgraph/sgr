import os

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