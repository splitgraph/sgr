from datetime import datetime as dt

from splitgraph.commandline import _conn
from splitgraph.commands import unmount
from test.splitgraph.conftest import PG_MNT, _mount_postgres, _mount_mysql, MYSQL_MNT


def test_mount_unmount():
    # It's up to us to commit the connection -- if we don't commit it, the actual test db doesn't get changed.
    conn = _conn()
    unmount(conn, PG_MNT)
    _mount_postgres(conn, PG_MNT)
    with conn.cursor() as cur:
        cur.execute("""SELECT * FROM "test/pg_mount".fruits""")
        assert (1, 'apple') in list(cur.fetchall())
    unmount(conn, PG_MNT)
    with conn.cursor() as cur:
        cur.execute("""SELECT * FROM information_schema.schemata where schema_name = '%s'""" % PG_MNT.to_schema())
        assert cur.fetchone() is None


def test_mount_mysql():
    conn = _conn()
    try:
        _mount_mysql(conn, MYSQL_MNT)
        with conn.cursor() as cur:
            cur.execute("""SELECT mushroom_id, name, discovery, friendly
                           FROM "test/mysql_mount".mushrooms
                           WHERE friendly = 0""")
            # Gotchas: bool coerced to int
            assert (2, 'deathcap', dt(2018, 3, 17, 8, 6, 26), 0) in list(cur.fetchall())
    finally:
        unmount(conn, MYSQL_MNT)


def test_cross_joins(sg_pg_mg_conn):
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT "test/pg_mount".fruits.fruit_id,
                             test_mg_mount.stuff.name,
                             "test/pg_mount".fruits.name as spirit_fruit
                      FROM "test/pg_mount".fruits 
                             JOIN test_mg_mount.stuff 
                             ON "test/pg_mount".fruits.fruit_id = test_mg_mount.stuff.duration""")
        assert cur.fetchall() == [(2, 'James', 'orange')]
