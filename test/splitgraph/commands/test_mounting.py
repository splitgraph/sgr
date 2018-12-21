from datetime import datetime as dt

from splitgraph import get_engine
from test.splitgraph.conftest import PG_MNT, _mount_postgres, _mount_mysql, MYSQL_MNT


def test_mount_unmount():
    PG_MNT.rm()
    _mount_postgres(PG_MNT)
    assert (1, 'apple') in get_engine().run_sql("""SELECT * FROM "test/pg_mount".fruits""")
    PG_MNT.rm()
    assert not get_engine().schema_exists(PG_MNT.to_schema())


def test_mount_mysql():
    try:
        _mount_mysql(MYSQL_MNT)
        # Gotchas: bool coerced to int
        assert (2, 'deathcap', dt(2018, 3, 17, 8, 6, 26), 0) in get_engine().run_sql(
            """SELECT mushroom_id, name, discovery, friendly
                           FROM "test/mysql_mount".mushrooms
                           WHERE friendly = 0""")
    finally:
        MYSQL_MNT.rm()


def test_cross_joins(local_engine_with_pg_and_mg):
    assert local_engine_with_pg_and_mg.run_sql(
        """SELECT "test/pg_mount".fruits.fruit_id,
                 test_mg_mount.stuff.name,
                 "test/pg_mount".fruits.name as spirit_fruit
           FROM "test/pg_mount".fruits 
           JOIN test_mg_mount.stuff 
           ON "test/pg_mount".fruits.fruit_id = test_mg_mount.stuff.duration""") == [(2, 'James', 'orange')]
