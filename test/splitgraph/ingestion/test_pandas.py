import contextlib
import os
from datetime import datetime as dt
from io import StringIO

import pytest

from splitgraph.core.types import TableColumn

with contextlib.suppress(ImportError):
    from splitgraph.ingestion.pandas import df_to_table, sql_to_df

    # If Pandas isn't installed, pytest will skip these tests
    # (see pytest.importorskip).

from test.splitgraph.conftest import INGESTION_RESOURCES_CSV, load_csv

pd = pytest.importorskip("pandas")
assert_frame_equal = pytest.importorskip("pandas.util.testing").assert_frame_equal


def _str_to_df(string, has_ts=True):
    df = pd.read_csv(
        StringIO(string),
        sep=",",
        index_col=0,
        parse_dates=["timestamp"] if has_ts else None,
        infer_datetime_format=True if has_ts else None,
    )
    df.columns = df.columns.str.strip()
    return df


base_df = _str_to_df(load_csv("base_df.csv"))

# 3 rows updated:
# 2: same (nothing changed)
# 3: changed timestamp
# 4: changed timestamp and name
upd_df_1 = _str_to_df(load_csv("patch_df.csv"))


def test_pandas_basic_insert(ingestion_test_repo):
    df_to_table(base_df, ingestion_test_repo, "test_table", if_exists="patch")
    ingestion_test_repo.commit()

    assert ingestion_test_repo.head.get_table("test_table").table_schema == [
        TableColumn(1, "fruit_id", "bigint", True),
        TableColumn(2, "timestamp", "timestamp without time zone", False),
        TableColumn(3, "name", "text", False),
    ]

    assert ingestion_test_repo.run_sql(
        "SELECT fruit_id, timestamp, name FROM test_table ORDER BY fruit_id"
    ) == [
        (1, dt(2018, 1, 1, 0, 11, 11), "apple"),
        (2, dt(2018, 1, 2, 0, 22, 22), "orange"),
        (3, dt(2018, 1, 3, 0, 33, 33), "mayonnaise"),
        (4, dt(2018, 1, 4, 0, 44, 44), "mustard"),
    ]


def test_pandas_no_processing_insert(ingestion_test_repo):
    # Make sure everything still works when we don't have a PK.
    df = pd.read_csv(os.path.join(INGESTION_RESOURCES_CSV, "base_df.csv"))
    df_to_table(df, ingestion_test_repo, "test_table")
    ingestion_test_repo.commit()

    assert ingestion_test_repo.head.get_table("test_table").table_schema == [
        TableColumn(1, "fruit_id", "bigint", False),
        TableColumn(2, "timestamp", "text", False),
        TableColumn(3, "name", "text", False),
    ]

    assert ingestion_test_repo.run_sql(
        "SELECT fruit_id, timestamp, name FROM test_table ORDER BY fruit_id"
    ) == [
        (1, "2018-01-01 00:11:11", "apple"),
        (2, "2018-01-02 00:22:22", "orange"),
        (3, "2018-01-03 00:33:33", "mayonnaise"),
        (4, "2018-01-04 00:44:44", "mustard"),
    ]


def test_pandas_update_replace(ingestion_test_repo):
    df_to_table(base_df, ingestion_test_repo, "test_table", if_exists="patch")
    old = ingestion_test_repo.commit()

    df_to_table(upd_df_1, ingestion_test_repo, "test_table", if_exists="replace")
    new = ingestion_test_repo.commit()

    old.checkout()
    new.checkout()

    assert ingestion_test_repo.run_sql(
        "SELECT fruit_id, timestamp, name FROM test_table ORDER BY fruit_id"
    ) == [
        (2, dt(2018, 1, 2, 0, 22, 22), "orange"),
        (3, dt(2018, 12, 31, 23, 59, 49), "mayonnaise"),
        (4, dt(2018, 12, 30, 0, 0), "chandelier"),
    ]

    # Since the table was replaced, we store it as a new snapshot instead of a patch.
    assert len(ingestion_test_repo.images["latest"].get_table("test_table").objects) == 1


def test_pandas_update_patch(ingestion_test_repo):
    df_to_table(base_df, ingestion_test_repo, "test_table", if_exists="patch")
    old = ingestion_test_repo.commit()

    df_to_table(upd_df_1, ingestion_test_repo, "test_table", if_exists="patch")
    new = ingestion_test_repo.commit()

    old.checkout()
    new.checkout()

    assert ingestion_test_repo.run_sql(
        "SELECT fruit_id, timestamp, name FROM test_table ORDER BY fruit_id"
    ) == [
        (1, dt(2018, 1, 1, 0, 11, 11), "apple"),
        (2, dt(2018, 1, 2, 0, 22, 22), "orange"),
        (3, dt(2018, 12, 31, 23, 59, 49), "mayonnaise"),
        (4, dt(2018, 12, 30, 0, 0), "chandelier"),
    ]

    assert sorted(ingestion_test_repo.diff("test_table", old, new)) == [
        (False, (3, dt(2018, 1, 3, 0, 33, 33), "mayonnaise")),
        (False, (4, dt(2018, 1, 4, 0, 44, 44), "mustard")),
        (True, (3, dt(2018, 12, 31, 23, 59, 49), "mayonnaise")),
        (True, (4, dt(2018, 12, 30, 0, 0), "chandelier")),
    ]


def test_pandas_update_different_schema(ingestion_test_repo):
    # Currently patches with dataframes with different columns are unsupported
    df_to_table(base_df, ingestion_test_repo, "test_table", if_exists="patch")
    ingestion_test_repo.commit()

    # Delete the 'timestamp' column
    truncated_df = upd_df_1["timestamp"]

    with pytest.raises(ValueError) as e:
        df_to_table(truncated_df, ingestion_test_repo, "test_table", if_exists="patch")
    assert "Schema changes are unsupported" in str(e.value)

    # Rename a column
    renamed_df = upd_df_1.copy()
    renamed_df.columns = ["timestamp", "name_rename"]

    with pytest.raises(ValueError) as e:
        df_to_table(renamed_df, ingestion_test_repo, "test_table", if_exists="patch")
    assert "Schema changes are unsupported" in str(e.value)


def test_evil_pandas_dataframes(ingestion_test_repo):
    # Test corner cases we found in the real world
    df = _str_to_df(load_csv("evil_df.csv"), has_ts=False)
    df_to_table(df, ingestion_test_repo, "test_table", if_exists="patch")

    assert ingestion_test_repo.run_sql(
        "SELECT id, job_title, some_number FROM test_table ORDER BY id"
    ) == [
        # Make sure backslashes don't break ingestion -- not exactly sure what the intention
        # in the original dataset was (job title is "PRESIDENT\").
        (1, "PRESIDENT\\", 25),
        # Test characters that can be used as separators still make it into fields
        (2, "\t", 26),
    ]


def test_pandas_update_type_changes_weaker(ingestion_test_repo):
    df_to_table(base_df, ingestion_test_repo, "test_table", if_exists="patch")
    ingestion_test_repo.commit()
    altered_df = upd_df_1.copy()
    altered_df["name"] = [4, 5, 6]
    altered_df["name"] = altered_df["name"].astype(int)

    # Type changes are passed through to Postgres to see if it can coerce them -- in this case
    # 'name' remains a string, so a string '4' is written for fruit_id = 2.
    df_to_table(altered_df, ingestion_test_repo, "test_table", if_exists="patch")
    assert ingestion_test_repo.run_sql("SELECT name FROM test_table WHERE fruit_id = 2") == [("4",)]


def test_pandas_update_type_changes_stricter(ingestion_test_repo):
    df_to_table(base_df, ingestion_test_repo, "test_table", if_exists="patch")
    ingestion_test_repo.commit()
    altered_df = upd_df_1.copy()
    altered_df.index = altered_df.index.map(str)

    # fruit_id (integer) is a string in this case -- Postgres should try to coerce
    # it back into integer to patch into the target table.

    df_to_table(altered_df, ingestion_test_repo, "test_table", if_exists="patch")
    assert ingestion_test_repo.run_sql("SELECT name FROM test_table WHERE fruit_id = 4") == [
        ("chandelier",)
    ]


def test_pandas_read_basic(ingestion_test_repo):
    df_to_table(base_df, ingestion_test_repo, "test_table", if_exists="patch")
    ingestion_test_repo.commit()

    # We currently don't detect the index column name since it's an arbitrary query that's passed.
    output = sql_to_df(
        "SELECT * FROM test_table", repository=ingestion_test_repo, index_col="fruit_id"
    )

    assert_frame_equal(base_df, output)


def test_pandas_read_other_checkout(ingestion_test_repo):
    df_to_table(base_df, ingestion_test_repo, "test_table", if_exists="patch")
    old = ingestion_test_repo.commit()
    df_to_table(upd_df_1, ingestion_test_repo, "test_table", if_exists="patch")
    new = ingestion_test_repo.commit()

    # Record the second version of the table
    patched_df = sql_to_df("SELECT * FROM test_table", repository=ingestion_test_repo)

    # Check out the old version but run the query against the new image -- new version should come out.
    old.checkout()
    output_1 = sql_to_df("SELECT * FROM test_table", image=new)
    assert_frame_equal(output_1, patched_df)

    # Test works with hashes
    output_2 = sql_to_df(
        "SELECT * FROM test_table", image=new.image_hash[:10], repository=ingestion_test_repo
    )
    assert_frame_equal(output_2, patched_df)


def test_pandas_read_lq_checkout(ingestion_test_repo):
    df_to_table(base_df, ingestion_test_repo, "test_table", if_exists="patch")
    old = ingestion_test_repo.commit()
    # Record a DF with just the values for fruit_id = 3 and 4 (ones that will be updated).
    query = "SELECT * FROM test_table WHERE fruit_id IN (3, 4)"

    old_3_4 = sql_to_df(query, repository=ingestion_test_repo)

    df_to_table(upd_df_1, ingestion_test_repo, "test_table", if_exists="patch")
    new = ingestion_test_repo.commit()
    new_3_4 = sql_to_df(query, repository=ingestion_test_repo)

    # Uncheckout the repo but don't delete it.
    ingestion_test_repo.uncheckout()
    assert ingestion_test_repo.head is None

    output_old = sql_to_df(query, image=old, use_lq=True)
    assert_frame_equal(old_3_4, output_old)

    output_new = sql_to_df(query, image=new, use_lq=True)
    assert_frame_equal(new_3_4, output_new)

    # Make sure we didn't do an actual checkout.
    assert ingestion_test_repo.head is None


def test_pandas_read_roundtripping(ingestion_test_repo):
    df_to_table(base_df, ingestion_test_repo, "test_table", if_exists="patch")
    ingestion_test_repo.commit()
    df_to_table(upd_df_1, ingestion_test_repo, "test_table", if_exists="patch")
    new = ingestion_test_repo.commit()

    df = sql_to_df("SELECT * FROM test_table", repository=ingestion_test_repo, index_col="fruit_id")

    # Pandas update syntax: update index 4 (fruit ID) to have a new timestamp.
    df.at[4, "timestamp"] = dt(2018, 1, 1, 1, 1, 1)

    # Write the whole df back in patch mode -- despite that, the changeset will only contain the updated cell.
    df_to_table(df, ingestion_test_repo, "test_table", if_exists="patch")
    new_2 = ingestion_test_repo.commit()

    assert ingestion_test_repo.diff("test_table", new, new_2) == [
        (False, (4, dt(2018, 12, 30, 0, 0), "chandelier")),
        (True, (4, dt(2018, 1, 1, 1, 1, 1), "chandelier")),
    ]


def test_pandas_kv(ingestion_test_repo):
    # Test reads and writes with a key-value type dataframe.
    df = pd.read_csv(os.path.join(INGESTION_RESOURCES_CSV, "base_df_kv.csv"), index_col=0)
    df_to_table(df, ingestion_test_repo, "test_table")

    # Test patching works without specifying an index col
    df = pd.read_csv(os.path.join(INGESTION_RESOURCES_CSV, "patch_df_kv.csv"))
    df_to_table(df, ingestion_test_repo, "test_table", if_exists="patch")

    assert_frame_equal(
        sql_to_df("SELECT * FROM test_table", repository=ingestion_test_repo),
        pd.DataFrame(
            data=[(1, "banana"), (2, "kumquat"), (3, "pendulum")], columns=["key", "value"]
        ),
    )
