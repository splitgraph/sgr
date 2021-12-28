import os
import shlex
import subprocess
from datetime import datetime as dt
from test.splitgraph.conftest import INGESTION_RESOURCES, INGESTION_RESOURCES_CSV

import pytest
from click.testing import CliRunner

from splitgraph.commandline.ingestion import csv_export, csv_import
from splitgraph.core.types import TableColumn


@pytest.mark.parametrize("custom_separator", [False, True])
def test_import_empty(ingestion_test_repo, custom_separator):
    runner = CliRunner()
    filename = "base_df.csv" if not custom_separator else "separator_df.csv"
    # Import without PKs/type conversion.
    result = runner.invoke(
        csv_import,
        [
            str(ingestion_test_repo),
            "test_table",
            "-f",
            os.path.join(INGESTION_RESOURCES_CSV, filename),
        ]
        + (["--separator", ";"] if custom_separator else []),
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    # Check table got created.
    assert ingestion_test_repo.diff("test_table", ingestion_test_repo.head, None) is True

    # Check a line in the table
    assert ingestion_test_repo.run_sql("SELECT * FROM test_table WHERE fruit_id = 4") == [
        (4, dt(2018, 1, 4, 0, 44, 44), "mustard")
    ]


def test_import_stdin(ingestion_test_repo):
    # Test that piping CSV data from stdin (where we don't support seek()) works
    # and infers the schema.

    # Grab the RDU weather CSV from the examples directory
    csv_path = os.path.normpath(
        os.path.join(
            INGESTION_RESOURCES, "../../../examples/import-from-csv/rdu-weather-history.csv"
        )
    )
    assert os.path.exists(csv_path)

    # Run the import directly from the shell to make sure Click's CliRunner isn't doing
    # anything funky with the file + use cat / pipe to make sure we don't just set a file
    # descriptor as stdin.
    subprocess.check_call(
        "cat %s | sgr --verbosity DEBUG "
        'csv import -k date -t date timestamp --separator ";" %s rdu'
        % (shlex.quote(csv_path), shlex.quote(ingestion_test_repo.to_schema())),
        shell=True,
    )

    assert ingestion_test_repo.run_sql("SELECT COUNT(1) FROM rdu") == [(4633,)]
    assert ingestion_test_repo.run_sql(
        "SELECT fastest2minwinddir FROM rdu WHERE date = '2011-04-18'"
    ) == [(220,)]

    # Check the first few columns to make sure we inferred the schema correctly (remainder are all
    # Yes/No varchars)
    assert ingestion_test_repo.engine.get_full_table_schema(ingestion_test_repo.to_schema(), "rdu")[
        :12
    ] == [
        TableColumn(
            ordinal=1, name="date", pg_type="timestamp without time zone", is_pk=True, comment=None
        ),
        TableColumn(ordinal=2, name="temperaturemin", pg_type="numeric", is_pk=False, comment=None),
        TableColumn(ordinal=3, name="temperaturemax", pg_type="numeric", is_pk=False, comment=None),
        TableColumn(ordinal=4, name="precipitation", pg_type="numeric", is_pk=False, comment=None),
        TableColumn(ordinal=5, name="snowfall", pg_type="numeric", is_pk=False, comment=None),
        TableColumn(ordinal=6, name="snowdepth", pg_type="numeric", is_pk=False, comment=None),
        TableColumn(ordinal=7, name="avgwindspeed", pg_type="numeric", is_pk=False, comment=None),
        TableColumn(
            ordinal=8, name="fastest2minwinddir", pg_type="integer", is_pk=False, comment=None
        ),
        TableColumn(
            ordinal=9, name="fastest2minwindspeed", pg_type="numeric", is_pk=False, comment=None
        ),
        TableColumn(
            ordinal=10, name="fastest5secwinddir", pg_type="integer", is_pk=False, comment=None
        ),
        TableColumn(
            ordinal=11, name="fastest5secwindspeed", pg_type="numeric", is_pk=False, comment=None
        ),
        TableColumn(ordinal=12, name="fog", pg_type="character varying", is_pk=False, comment=None),
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
            os.path.join(INGESTION_RESOURCES_CSV, "base_df.csv"),
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
            os.path.join(INGESTION_RESOURCES_CSV, "base_df.csv"),
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
            os.path.join(INGESTION_RESOURCES_CSV, "patch_df.csv"),
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
            os.path.join(INGESTION_RESOURCES_CSV, "base_df.csv"),
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
            os.path.join(INGESTION_RESOURCES_CSV, "base_df.csv"),
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
            os.path.join(INGESTION_RESOURCES_CSV, "patch_df.csv"),
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
