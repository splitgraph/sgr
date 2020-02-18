import os
import subprocess

from click.testing import CliRunner

from splitgraph.__version__ import __version__
from splitgraph.commandline import init_c
from splitgraph.core.common import get_metadata_schema_version
from splitgraph.core.engine import init_engine
from splitgraph.engine import get_engine, ResultShape


def test_init_new_db():
    try:
        get_engine().delete_database("testdb")

        # CliRunner doesn't run in a brand new process and by that point PG_DB has propagated
        # through a few modules that are difficult to patch out, so let's just shell out.
        output = subprocess.check_output(
            "SG_LOGLEVEL=INFO SG_ENGINE_DB_NAME=testdb sgr init",
            shell=True,
            stderr=subprocess.STDOUT,
        )
        output = output.decode("utf-8")
        assert "Creating database testdb" in output
        assert "Installing the audit trigger" in output
    finally:
        get_engine().delete_database("testdb")


def test_init_skip_object_handling_version_():
    # Test engine initialization where we don't install an audit trigger + also
    # check that the schema version history table is maintained.

    runner = CliRunner()
    engine = get_engine()

    schema_version, date_installed = get_metadata_schema_version(engine)
    assert schema_version == __version__

    try:
        engine.run_sql("DROP SCHEMA IF EXISTS audit CASCADE")
        engine.run_sql("DROP FUNCTION IF EXISTS splitgraph_api.upload_object")
        assert not engine.schema_exists("audit")
        result = runner.invoke(init_c, ["--skip-object-handling"])
        assert result.exit_code == 0
        assert not engine.schema_exists("audit")
        assert (
            engine.run_sql(
                "SELECT COUNT(*) FROM information_schema.routines "
                "WHERE routine_schema = 'splitgraph_api' "
                "AND routine_name = 'upload_object'",
                return_shape=ResultShape.ONE_ONE,
            )
            == 0
        )
    finally:
        init_engine(skip_object_handling=False)
        schema_version_new, date_installed_new = get_metadata_schema_version(engine)

        # No migrations currently -- check the current version hasn't changed.
        assert schema_version == schema_version_new
        assert date_installed == date_installed_new

        assert engine.schema_exists("audit")
        assert (
            engine.run_sql(
                "SELECT COUNT(*) FROM information_schema.routines "
                "WHERE routine_schema = 'splitgraph_api' "
                "AND routine_name = 'upload_object'",
                return_shape=ResultShape.ONE_ONE,
            )
            == 1
        )


def test_init_override_engine():
    # Doesn't really test that all of the overridden engine's config makes it into the Engine object that
    # initialize() is called on but that's tested implicitly throughout the rest of the suite: here, since
    # initialize() logs the engine it uses, check that the remote engine is being initialized.

    # Inject the config here. If this check_output breaks (with something like "KeyError: 'remotes' not in CONFIG"),
    # this path is probably the culprit.
    output = subprocess.check_output(
        "SG_CONFIG_FILE=%s SG_LOGLEVEL=INFO SG_ENGINE=remote_engine sgr init"
        % os.path.join(os.path.dirname(__file__), "../../resources/.sgconfig"),
        shell=True,
        stderr=subprocess.STDOUT,
    )
    output = output.decode("utf-8")
    assert str(get_engine("remote_engine")) in output
