import os
from datetime import datetime as dt

from click.testing import CliRunner

from splitgraph.ingestion import csv_import, csv_export
from test.splitgraph.conftest import INGESTION_RESOURCES


def test_import_empty(ingestion_test_repo):
    runner = CliRunner()
    # Import without PKs/type conversion.
    result = runner.invoke(csv_import, [str(ingestion_test_repo), 'test_table', '-f',
                                        os.path.join(INGESTION_RESOURCES, 'base_df.csv')])

    assert result.exit_code == 0
    # Check table got created.
    assert ingestion_test_repo.diff('test_table', ingestion_test_repo.head, None) is True

    # Check a line in the table
    assert ingestion_test_repo.run_sql("SELECT * FROM test_table WHERE fruit_id = 4") \
           == [(4, '2018-01-04 00:44:44', 'mustard')]


def test_import_empty_pk_datetimes(ingestion_test_repo):
    runner = CliRunner()
    # This time, import so that the new table has PKs and the datetimes are parsed.
    result = runner.invoke(csv_import, [str(ingestion_test_repo), 'test_table', '-f',
                                        os.path.join(INGESTION_RESOURCES, 'base_df.csv'),
                                        '-k', 'fruit_id', '-d', 'timestamp'])
    assert result.exit_code == 0
    # Check a line in the table
    assert ingestion_test_repo.run_sql("SELECT * FROM test_table WHERE fruit_id = 4") \
           == [(4, dt(2018, 1, 4, 0, 44, 44), 'mustard')]
    assert ingestion_test_repo.engine.get_primary_keys(str(ingestion_test_repo), 'test_table') == [
        ('fruit_id', 'bigint')]


def test_import_patch(ingestion_test_repo):
    runner = CliRunner()
    runner.invoke(csv_import, [str(ingestion_test_repo), 'test_table', '-f',
                               os.path.join(INGESTION_RESOURCES, 'base_df.csv'),
                               '-k', 'fruit_id', '-d', 'timestamp'])
    ingestion_test_repo.commit()

    result = runner.invoke(csv_import, [str(ingestion_test_repo), 'test_table', '-f',
                                        os.path.join(INGESTION_RESOURCES, 'patch_df.csv'),
                                        '-k', 'fruit_id', '-d', 'timestamp'],
                           catch_exceptions=False)
    assert result.exit_code == 0
    assert ingestion_test_repo.run_sql("SELECT * FROM test_table WHERE fruit_id = 4") == [
        (4, dt(2018, 12, 30), 'chandelier')]


def test_export_basic(ingestion_test_repo):
    runner = CliRunner()
    runner.invoke(csv_import, [str(ingestion_test_repo), 'test_table', '-f',
                               os.path.join(INGESTION_RESOURCES, 'base_df.csv'),
                               '-k', 'fruit_id', '-d', 'timestamp'])
    ingestion_test_repo.commit()

    result = runner.invoke(csv_export, [str(ingestion_test_repo) + ':' + ingestion_test_repo.head.image_hash[:10],
                                        'SELECT * FROM test_table WHERE fruit_id = 4'])
    assert result.exit_code == 0
    assert result.stdout == 'fruit_id,timestamp,name\n4,2018-01-04 00:44:44,mustard\n'


def test_export_lq(ingestion_test_repo):
    runner = CliRunner()
    runner.invoke(csv_import, [str(ingestion_test_repo), 'test_table', '-f',
                               os.path.join(INGESTION_RESOURCES, 'base_df.csv'),
                               '-k', 'fruit_id', '-d', 'timestamp'])
    old = ingestion_test_repo.commit()
    runner.invoke(csv_import, [str(ingestion_test_repo), 'test_table', '-f',
                               os.path.join(INGESTION_RESOURCES, 'patch_df.csv'),
                               '-k', 'fruit_id', '-d', 'timestamp'])
    new = ingestion_test_repo.commit()
    ingestion_test_repo.delete(uncheckout=True, unregister=False)

    # Check old value
    result = runner.invoke(csv_export, [str(ingestion_test_repo) + ':' + old.image_hash[:10],
                                        'SELECT * FROM test_table WHERE fruit_id = 4', '--layered'])
    assert result.exit_code == 0
    assert result.stdout == 'fruit_id,timestamp,name\n4,2018-01-04 00:44:44,mustard\n'

    # Check new value
    result = runner.invoke(csv_export, [str(ingestion_test_repo) + ':' + new.image_hash[:10],
                                        'SELECT * FROM test_table WHERE fruit_id = 4', '--layered'])
    assert result.exit_code == 0
    # datetime format not preserved (was 2018-12-30 00:00:00) but we can't detect what it was anyway
    # without the user passing it in.
    assert result.stdout == 'fruit_id,timestamp,name\n4,2018-12-30,chandelier\n'
