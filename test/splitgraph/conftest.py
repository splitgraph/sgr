import pytest

from splitgraph.commandline import _conn
from splitgraph.commands import *
from splitgraph.commands import unmount

PG_MNT = 'test_pg_mount'
MG_MNT = 'test_mg_mount'


def _mount_postgres(conn, mountpoint):
    mount(conn, server='pgorigin', port=5432, username='originro', password='originpass', mountpoint=mountpoint,
          mount_handler='postgres_fdw', extra_options={"dbname": "origindb", "remote_schema": "public"})


def _mount_mongo(conn, mountpoint):
    mount(conn, server='mongoorigin', port=27017, username='originro', password='originpass', mountpoint=mountpoint,
          mount_handler='mongo_fdw', extra_options={"stuff": {
            "db": "origindb",
            "coll": "stuff",
            "schema": {
                "name": "text",
                "duration": "numeric",
                "happy": "boolean"
            }}})


@pytest.fixture
def sg_pg_conn():
    # SG connection with a mounted Postgres db
    conn = _conn()
    unmount(conn, PG_MNT)
    unmount(conn, PG_MNT + '_pull')
    unmount(conn, 'output')
    _mount_postgres(conn, PG_MNT)
    yield conn
    unmount(conn, PG_MNT)
    unmount(conn, PG_MNT + '_pull')
    unmount(conn, 'output')
    conn.close()


@pytest.fixture
def sg_pg_mg_conn():
    # SG connection with a mounted Mongo + Postgres db
    # Also, remove the 'output' mountpoint that we'll be using in the sgfile tests
    conn = _conn()
    unmount(conn, MG_MNT)
    unmount(conn, PG_MNT)
    unmount(conn, PG_MNT + '_pull')
    unmount(conn, 'output')
    _mount_postgres(conn, PG_MNT)
    _mount_mongo(conn, MG_MNT)
    yield conn
    unmount(conn, MG_MNT)
    unmount(conn, PG_MNT)
    unmount(conn, PG_MNT + '_pull')
    unmount(conn, 'output')
    conn.close()


