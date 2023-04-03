from splitgraph.ingestion.sqlite import get_select_query, sqlite_to_postgres_type


def test_type_mapping():
    assert sqlite_to_postgres_type("INT") == "INTEGER"
    assert sqlite_to_postgres_type("TEXT") == "TEXT"
    assert sqlite_to_postgres_type("text") == "TEXT"
    assert sqlite_to_postgres_type("varchar(255)") == "VARCHAR(255)"
    assert sqlite_to_postgres_type("VARCHAR(255)") == "VARCHAR(255)"
    assert sqlite_to_postgres_type("REAL") == "REAL"
    assert sqlite_to_postgres_type("real") == "REAL"
    assert sqlite_to_postgres_type("decimal(2,10)") == "DECIMAL(2,10)"
    assert sqlite_to_postgres_type("decimal(20,100)") == "DECIMAL(20,100)"
    assert sqlite_to_postgres_type("BLOB") == "BYTEA"
    assert sqlite_to_postgres_type("decimal(2,    20)") == "DECIMAL(2,20)"
    assert sqlite_to_postgres_type("NATIVE CHARACTER(70)") == "VARCHAR(70)"
    assert sqlite_to_postgres_type("NVARCHAR(160)") == "VARCHAR(160)"


def test_sqlite_select_query():
    # explicit pk, no previous batch
    assert get_select_query("my_table", ["id"], None, 10) == (
        'SELECT * FROM "my_table" WHERE true ORDER BY "id" ASC LIMIT 10',
        {},
    )
    # implicit pk, no previous batch
    assert get_select_query("my_table", [], None, 10) == (
        'SELECT ROWID, * FROM "my_table" WHERE true ORDER BY "ROWID" ASC LIMIT 10',
        {},
    )
    # explicit pk, has previous batch
    assert get_select_query("my_table", ["id1", "id2"], {"id1": 0, "id2": "a"}, 10) == (
        'SELECT * FROM "my_table" WHERE ("id1", "id2") > (%s, %s) ORDER BY "id1", "id2" ASC LIMIT 10',
        {"id1": 0, "id2": "a"},
    )
    # implicit pk, has previous batch
    assert get_select_query("my_table", [], {"ROWID": 500}, 10) == (
        'SELECT ROWID, * FROM "my_table" WHERE ("ROWID") > (%s) ORDER BY "ROWID" ASC LIMIT 10',
        {"ROWID": 500},
    )
