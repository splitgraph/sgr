import os
from datetime import datetime as dt

import pytest
from click.testing import CliRunner
from test.splitgraph.conftest import INGESTION_RESOURCES

from splitgraph.commandline.ingestion import csv_import, csv_export


@pytest.mark.parametrize("custom_separator", [False, True])
def test_import_empty(ingestion_test_repo, custom_separator):
    runner = CliRunner()
    filename = "base_df.csv" if not custom_separator else "separator_df.csv"
    # Import without PKs/type conversion.
    result = runner.invoke(
        csv_import,
        [str(ingestion_test_repo), "test_table", "-f", os.path.join(INGESTION_RESOURCES, filename),]
        + (["--separator", ";"] if custom_separator else []),
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    # Check table got created.
    assert ingestion_test_repo.diff("test_table", ingestion_test_repo.head, None) is True

    # Check a line in the table
    assert ingestion_test_repo.run_sql("SELECT * FROM test_table WHERE fruit_id = 4") == [
        (4, "2018-01-04 00:44:44", "mustard")
    ]


def test_import_empty_pk_datetimes(ingestion_test_repo):
    runner = CliRunner()
    # This time, import so that the new table has PKs and the datetimes are parsed.
    result = runner.invoke(
        csv_import,
        [
            str(ingestion_test_repo),
            "test_table",
            "-f",
            os.path.join(INGESTION_RESOURCES, "base_df.csv"),
            "-k",
            "fruit_id",
            # Set type of the "timestamp" col to timestamp
            "-t",
            "timestamp",
            "timestamp",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    # Check a line in the table
    assert ingestion_test_repo.run_sql("SELECT * FROM test_table WHERE fruit_id = 4") == [
        (4, dt(2018, 1, 4, 0, 44, 44), "mustard")
    ]
    assert ingestion_test_repo.engine.get_primary_keys(str(ingestion_test_repo), "test_table") == [
        ("fruit_id", "integer")
    ]


def test_import_patch(ingestion_test_repo):
    runner = CliRunner()
    runner.invoke(
        csv_import,
        [
            str(ingestion_test_repo),
            "test_table",
            "-f",
            os.path.join(INGESTION_RESOURCES, "base_df.csv"),
            "-k",
            "fruit_id",
            "-t",
            "timestamp",
            "timestamp",
        ],
        catch_exceptions=False,
    )
    ingestion_test_repo.commit()

    result = runner.invoke(
        csv_import,
        [
            str(ingestion_test_repo),
            "test_table",
            "-f",
            os.path.join(INGESTION_RESOURCES, "patch_df.csv"),
            "-k",
            "fruit_id",
            "-t",
            "timestamp",
            "timestamp",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert ingestion_test_repo.run_sql("SELECT * FROM test_table WHERE fruit_id = 4") == [
        (4, dt(2018, 12, 30), "chandelier")
    ]


def test_export_basic(ingestion_test_repo):
    runner = CliRunner()
    runner.invoke(
        csv_import,
        [
            str(ingestion_test_repo),
            "test_table",
            "-f",
            os.path.join(INGESTION_RESOURCES, "base_df.csv"),
            "-k",
            "fruit_id",
            "-t",
            "timestamp",
            "timestamp",
        ],
        catch_exceptions=False,
    )
    ingestion_test_repo.commit()

    result = runner.invoke(
        csv_export,
        [
            str(ingestion_test_repo) + ":" + ingestion_test_repo.head.image_hash[:10],
            "SELECT * FROM test_table WHERE fruit_id = 4",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert result.stdout == "fruit_id,timestamp,name\n4,2018-01-04 00:44:44,mustard\n"


def test_export_lq(ingestion_test_repo):
    runner = CliRunner()
    runner.invoke(
        csv_import,
        [
            str(ingestion_test_repo),
            "test_table",
            "-f",
            os.path.join(INGESTION_RESOURCES, "base_df.csv"),
            "-k",
            "fruit_id",
            "-t",
            "timestamp",
            "timestamp",
        ],
        catch_exceptions=False,
    )
    old = ingestion_test_repo.commit()
    runner.invoke(
        csv_import,
        [
            str(ingestion_test_repo),
            "test_table",
            "-f",
            os.path.join(INGESTION_RESOURCES, "patch_df.csv"),
            "-k",
            "fruit_id",
            "-t",
            "timestamp",
            "timestamp",
        ],
        catch_exceptions=False,
    )
    new = ingestion_test_repo.commit()
    ingestion_test_repo.delete(uncheckout=True, unregister=False)

    # Check old value
    result = runner.invoke(
        csv_export,
        [
            str(ingestion_test_repo) + ":" + old.image_hash[:10],
            "SELECT * FROM test_table WHERE fruit_id = 4",
            "--layered",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert result.stdout == "fruit_id,timestamp,name\n4,2018-01-04 00:44:44,mustard\n"

    # Check new value
    result = runner.invoke(
        csv_export,
        [
            str(ingestion_test_repo) + ":" + new.image_hash[:10],
            "SELECT * FROM test_table WHERE fruit_id = 4",
            "--layered",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert result.stdout == "fruit_id,timestamp,name\n4,2018-12-30 00:00:00,chandelier\n"
