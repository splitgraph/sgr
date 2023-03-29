from splitgraph.ingestion.sqlite import sqlite_to_postgres_type


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
    assert sqlite_to_postgres_type("BLOB") == "BLOB"
    assert sqlite_to_postgres_type("decimal(2,    20)") == "DECIMAL(2,20)"
    assert sqlite_to_postgres_type("NATIVE CHARACTER(70)") == "VARCHAR(70)"
    assert sqlite_to_postgres_type("NVARCHAR(160)") == "VARCHAR(160)"
