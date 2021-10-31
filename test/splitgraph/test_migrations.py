from datetime import datetime
from unittest.mock import MagicMock

import pytest
from splitgraph.core.migration import source_files_to_apply
from splitgraph.engine.postgres.engine import PsycopgEngine


@pytest.mark.parametrize(
    ("files", "current_version", "target_version", "expected_files"),
    [
        # No schema installed at all
        (["test--0.0.1.sql"], None, None, ["test--0.0.1.sql"]),
        # No schema installed, check latest is installed
        (["test--0.0.1.sql", "test--0.0.2.sql"], None, None, ["test--0.0.2.sql"]),
        # No schema installed, check the one we asked for gets installed
        (["test--0.0.1.sql", "test--0.0.2.sql"], None, "0.0.1", ["test--0.0.1.sql"]),
        # No change required
        (["test--0.0.1.sql", "test--0.0.2.sql"], "0.0.1", "0.0.1", []),
        # Only have initial installation files and no migrations, can't upgrade .1 to .2
        (["test--0.0.1.sql", "test--0.0.2.sql"], "0.0.1", "0.0.2", ValueError),
        # Check can find path from .1 to .2
        (
            ["test--0.0.1.sql", "test--0.0.1--0.0.2.sql"],
            "0.0.1",
            "0.0.2",
            ["test--0.0.1--0.0.2.sql"],
        ),
        # Check chooses shortest path from 0.0.1 to 1.0
        (
            [
                "test--0.0.1.sql",
                "test--0.0.1--0.0.2.sql",
                "test--0.0.2--1.0.sql",
                "test--0.0.1--1.0.sql",
            ],
            "0.0.1",
            None,
            ["test--0.0.1--1.0.sql"],
        ),
        # Check chooses path with multiple points
        (
            [
                "test--0.0.1.sql",
                "test--0.0.1--0.0.2.sql",
                "test--0.0.2--1.0.sql",
                "test--0.0.1--1.0.sql",
                "test--0.0.2--0.0.3.sql",
            ],
            "0.0.1",
            "0.0.3",
            ["test--0.0.1--0.0.2.sql", "test--0.0.2--0.0.3.sql"],
        ),
        # Check allows downgrades (also a test for loops in the graph)
        (
            [
                "test--0.0.1.sql",
                "test--0.0.1--0.0.2.sql",
                "test--0.0.2--1.0.sql",
                "test--0.0.1--1.0.sql",
                "test--0.0.2--0.0.3.sql",
                "test--1.0--0.0.1.sql",
            ],
            "1.0",
            "0.0.3",
            [
                "test--1.0--0.0.1.sql",
                "test--0.0.1--0.0.2.sql",
                "test--0.0.2--0.0.3.sql",
            ],
        ),
        # Check failure when can't find a path
        (
            [
                "test--0.0.1.sql",
                "test--0.0.1--0.0.2.sql",
                "test--0.0.2--1.0.sql",
                "test--0.0.1--1.0.sql",
            ],
            "0.0.1",
            "0.0.3",
            ValueError,
        ),
        # Check failure with unparseable filenames
        (
            ["test--0.0.1.sql", "test--0.0.1--0.0.2.sql", "test.sql"],
            "0.0.1",
            "0.0.2",
            ValueError,
        ),
        # Check schema that gets overwritten rather than upgraded even
        # if it's already installed with the same version
        (["test_static--0.0.1.sql"], "0.0.1", None, ["test_static--0.0.1.sql"]),
    ],
)
def test_source_files_to_apply(files, current_version, target_version, expected_files):
    schema_name = "test_static" if any("test_static" in f for f in files) else "test"
    installed_version = (current_version, datetime.utcnow()) if current_version else None

    engine = MagicMock(spec=PsycopgEngine)
    engine.run_sql.return_value = installed_version

    kwargs = {
        "engine": engine,
        "schema_files": files,
        "schema_name": schema_name,
        "target_version": target_version,
        "static": (schema_name == "test_static"),
    }

    if isinstance(expected_files, type):
        with pytest.raises(expected_files):
            source_files_to_apply(**kwargs)
    else:
        actual_files, version = source_files_to_apply(**kwargs)
        assert actual_files == expected_files
