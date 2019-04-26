import pytest

from splitgraph.core.sql import validate_splitfile_sql, validate_import_sql
from splitgraph.exceptions import UnsupportedSQLException


def succeeds_on_both(sql):
    validate_splitfile_sql(sql)
    validate_import_sql(sql)


def fails_on_both(sql):
    with pytest.raises(UnsupportedSQLException) as e:
        validate_splitfile_sql(sql)
    with pytest.raises(UnsupportedSQLException) as e:
        validate_import_sql(sql)


def succeeds_on_sql_fails_on_import(sql):
    validate_splitfile_sql(sql)
    with pytest.raises(UnsupportedSQLException) as e:
        validate_import_sql(sql)


def test_validate_select():
    succeeds_on_both("SELECT * FROM my_table")


def test_validate_select_where():
    succeeds_on_both("SELECT * FROM my_table WHERE a > 5")


def test_validate_select_cols():
    succeeds_on_both("SELECT a, b FROM my_table WHERE a > 5")


def test_validate_join():
    succeeds_on_both(
        "SELECT a, b FROM my_table JOIN my_other_table ON my_table.a = my_other_table.a WHERE a > 5"
    )


def test_validate_bool():
    succeeds_on_both("SELECT a, b FROM my_table WHERE a > 5 OR b < 3")


def test_validate_float():
    succeeds_on_both("SELECT a, b FROM my_table WHERE a > 5.0 OR b < 3.0")


def test_validate_null():
    succeeds_on_both("SELECT a, b FROM my_table WHERE a IS NOT NULL")


def test_validate_range():
    succeeds_on_both("SELECT a, b FROM my_table WHERE a BETWEEN 42 and 49")


def test_validate_coalesce():
    succeeds_on_both("SELECT COALESCE(a, 42), b FROM my_table")


def test_validate_alias():
    succeeds_on_both("SELECT a, b as bb FROM my_table")


def test_validate_order():
    succeeds_on_both("SELECT a, b FROM my_table WHERE a > 5 ORDER BY b DESC")


def test_validate_dates():
    succeeds_on_both("SELECT * FROM my_table WHERE a > b - interval '1 year'")


def test_validate_types():
    succeeds_on_both("SELECT a::integer, b::timestamp FROM my_table")


def test_validate_with():
    succeeds_on_both(
        "WITH t_a (a) AS (SELECT a FROM table_1) "
        "SELECT * FROM my_table JOIN t_a ON my_table.a = t_a.a"
    )


def test_validate_exists():
    succeeds_on_both(
        "SELECT a FROM table_1 WHERE EXISTS (SELECT 1 FROM table_2 WHERE table_2.a = table_1.a)"
    )


def test_validate_in():
    succeeds_on_both("SELECT a FROM table_1 WHERE b IN (1,2,3)")


def test_validate_array():
    succeeds_on_both("SELECT a FROM table_1 WHERE b = ANY (ARRAY [1,2,3])")


def test_validate_insert():
    succeeds_on_sql_fails_on_import("INSERT INTO my_table (a, b) VALUES (3, 4), (5, 6), (7+1, 8)")


def test_validate_insert_multiple():
    succeeds_on_sql_fails_on_import(
        "INSERT INTO my_table (a, b) VALUES (3, 4);" "INSERT INTO my_table VALUES (5, 6);"
    )


def test_validate_delete():
    succeeds_on_sql_fails_on_import("DELETE FROM table_1 WHERE a < b")


def test_validate_update():
    succeeds_on_sql_fails_on_import("UPDATE table_1 SET a = 1 WHERE b > a")


def test_validate_add_column():
    succeeds_on_sql_fails_on_import(
        "ALTER TABLE table_1 ADD COLUMN col1 TIMESTAMP DEFAULT '2012-01-01'"
    )


def test_validate_delete_column():
    succeeds_on_sql_fails_on_import("ALTER TABLE table_1 DROP COLUMN col1")


def test_validate_drop_table():
    succeeds_on_sql_fails_on_import("DROP TABLE table_1 CASCADE")


def test_validate_create_table():
    succeeds_on_sql_fails_on_import(
        "CREATE TABLE table_1 (key INTEGER PRIMARY KEY, value VARCHAR, value_2 JSON)"
    )


def test_validate_create_table_as():
    succeeds_on_sql_fails_on_import("CREATE TABLE table_1 AS select key + 2 FROM table_2")


def test_validate_multiple_selects():
    succeeds_on_sql_fails_on_import("SELECT * FROM test; SELECT * FROM test")


def test_validate_no_create_schema():
    fails_on_both("CREATE SCHEMA test")


def test_validate_no_lock_table():
    fails_on_both("LOCK TABLE table1 IN EXCLUSIVE MODE")


def test_validate_no_information_schema():
    fails_on_both("SELECT * FROM information_schema.tables")


def test_validate_no_schema():
    fails_on_both("SELECT a FROM other_schema.my_table")


def test_validate_no_function_call():
    fails_on_both("SELECT run_func(42)")


def test_validate_no_set():
    fails_on_both("SET search_path=some_schema;")
