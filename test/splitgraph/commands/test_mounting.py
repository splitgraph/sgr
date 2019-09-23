from datetime import datetime as dt

import pytest

from splitgraph.core.repository import Repository
from splitgraph.engine import get_engine
from test.splitgraph.conftest import _mount_postgres, _mount_mysql, _mount_mongo

PG_MNT = Repository.from_schema("test/pg_mount")
MG_MNT = Repository.from_schema("test_mg_mount")
MYSQL_MNT = Repository.from_schema("test/mysql_mount")


@pytest.mark.mounting
def test_mount_unmount(local_engine_empty):
    _mount_postgres(PG_MNT)
    assert (1, "apple") in get_engine().run_sql("""SELECT * FROM "test/pg_mount".fruits""")
    PG_MNT.delete()
    assert not get_engine().schema_exists(PG_MNT.to_schema())


@pytest.mark.mounting
def test_mount_partial(local_engine_empty):
    _mount_postgres(PG_MNT, tables=["fruits"])
    assert get_engine().table_exists(PG_MNT.to_schema(), "fruits")
    assert not get_engine().table_exists(PG_MNT.to_schema(), "vegetables")


@pytest.mark.mounting
def test_mount_mysql(local_engine_empty):
    try:
        _mount_mysql(MYSQL_MNT)
        # Gotchas: bool coerced to int
        assert (2, "deathcap", dt(2018, 3, 17, 8, 6, 26), 0) in get_engine().run_sql(
            """SELECT mushroom_id, name, discovery, friendly
                           FROM "test/mysql_mount".mushrooms
                           WHERE friendly = 0"""
        )
    finally:
        MYSQL_MNT.delete()


@pytest.mark.mounting
def test_cross_joins(local_engine_empty):
    _mount_postgres(PG_MNT)
    _mount_mongo(MG_MNT)
    assert (
        local_engine_empty.run_sql(
            """SELECT "test/pg_mount".fruits.fruit_id,
                 test_mg_mount.stuff.name,
                 "test/pg_mount".fruits.name as spirit_fruit
           FROM "test/pg_mount".fruits 
           JOIN test_mg_mount.stuff 
           ON "test/pg_mount".fruits.fruit_id = test_mg_mount.stuff.duration"""
        )
        == [(2, "James", "orange")]
    )
