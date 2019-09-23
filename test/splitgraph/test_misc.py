from datetime import datetime as dt
from unittest.mock import patch

import pytest
from psycopg2.errors import CheckViolation

from splitgraph.core.common import Tracer
from splitgraph.core.engine import lookup_repository, repository_exists
from splitgraph.core.metadata_manager import Object
from splitgraph.core.repository import Repository
from splitgraph.exceptions import RepositoryNotFoundError, EngineInitializationError


def test_repo_lookup_override(remote_engine):
    test_repo = Repository("overridden", "repo", engine=remote_engine)
    try:
        test_repo.init()
        assert lookup_repository("overridden/repo") == test_repo
    finally:
        test_repo.delete(unregister=True, uncheckout=True)


def test_repo_lookup_override_fail():
    with pytest.raises(RepositoryNotFoundError) as e:
        lookup_repository("does/not_exist")
    assert "Unknown repository" in str(e.value)


def test_tracer():
    with patch("splitgraph.core.common.datetime") as datetime:
        datetime.now.return_value = dt(2019, 1, 1)
        tracer = Tracer()

        datetime.now.return_value = dt(2019, 1, 1, 0, 0, 1)
        tracer.log("event_1")

        datetime.now.return_value = dt(2019, 1, 1, 0, 0, 30)
        tracer.log("event_2")

    assert tracer.get_total_time() == 30
    assert tracer.get_durations() == [("event_1", 1.0), ("event_2", 29.0)]
    assert (
        str(tracer)
        == """event_1: 1.000
event_2: 29.000
Total: 30.000"""
    )


def test_metadata_constraints_image_hashes(local_engine_empty):
    R = Repository("some", "repo")
    with pytest.raises(CheckViolation):
        R.images.add(parent_id="0" * 64, image="bad_hash")

    with pytest.raises(CheckViolation):
        R.images.add(parent_id="bad_hash", image="cafecafe" * 8)

    with pytest.raises(CheckViolation):
        R.images.add(parent_id="cafecafe" * 8, image="cafecafe" * 7)

    with pytest.raises(CheckViolation):
        R.images.add(parent_id="cafecafe" * 8, image="cafecafe" * 8)


def test_metadata_constraints_object_ids_hashes(local_engine_empty):
    R = Repository("some", "repo")
    R.images.add(parent_id="0" * 64, image="cafecafe" * 8)
    R.commit_engines()

    with pytest.raises(CheckViolation):
        R.objects.register_objects(
            [
                Object(
                    object_id="broken",
                    format="FRAG",
                    namespace="",
                    size=42,
                    insertion_hash="0" * 64,
                    deletion_hash="0" * 64,
                    object_index={},
                )
            ]
        )

    with pytest.raises(CheckViolation):
        R.objects.register_objects(
            [
                Object(
                    object_id="o12345",
                    format="FRAG",
                    namespace="",
                    size=42,
                    insertion_hash="0" * 64,
                    deletion_hash="0" * 64,
                    object_index={},
                )
            ]
        )

    with pytest.raises(CheckViolation):
        R.objects.register_objects(
            [
                Object(
                    object_id="o" + "a" * 61 + "Z",
                    format="FRAG",
                    namespace="",
                    size=42,
                    insertion_hash="0" * 64,
                    deletion_hash="0" * 64,
                    object_index={},
                )
            ]
        )

    with pytest.raises(CheckViolation):
        R.objects.register_objects(
            [
                Object(
                    object_id="o" + "a" * 62,
                    format="FRAG",
                    namespace="",
                    size=42,
                    insertion_hash="broken",
                    deletion_hash="0" * 64,
                    object_index={},
                )
            ]
        )

    with pytest.raises(CheckViolation):
        R.objects.register_objects(
            [
                Object(
                    object_id="o" + "a" * 62,
                    format="FRAG",
                    namespace="",
                    size=42,
                    insertion_hash="0" * 64,
                    deletion_hash="broken",
                    object_index={},
                )
            ]
        )


def test_metadata_constraints_table_objects(local_engine_empty):
    R = Repository("some", "repo")
    R.images.add(parent_id="0" * 64, image="cafecafe" * 8)
    R.objects.register_objects(
        [
            Object(
                object_id="o" + "a" * 62,
                format="FRAG",
                namespace="",
                size=42,
                insertion_hash="0" * 64,
                deletion_hash="0" * 64,
                object_index={},
            )
        ]
    )
    R.commit_engines()

    with pytest.raises(CheckViolation) as e:
        R.objects.register_tables(
            R, [("cafecafe" * 8, "table", [(1, "key", "integer", True)], ["object_doesnt_exist"])]
        )

        assert "Some objects in the object_ids array aren''t registered!" in str(e)

    with pytest.raises(CheckViolation) as e:
        R.objects.register_tables(
            R,
            [
                (
                    "cafecafe" * 8,
                    "table",
                    [(1, "key", "integer", True)],
                    ["o" + "a" * 62, "previous_object_existed_but_this_one_doesnt"],
                )
            ],
        )

        assert "Some objects in the object_ids array aren''t registered!" in str(e)


def test_remove_with_no_audit_triggers(local_engine_empty):
    # Test deleting a repository with uncheckout=True doesn't fail if the engine
    # doesn't support checkouts to begin with: used if someone does "sgr rm -r [registry]"
    try:
        repo = Repository("some", "repo")
        repo.init()
        local_engine_empty.run_sql("DROP SCHEMA audit CASCADE")
        local_engine_empty.commit()
        # This still raises
        with pytest.raises(EngineInitializationError):
            local_engine_empty.discard_pending_changes("some/repo")

        # This doesn't.
        assert repository_exists(repo)
        Repository("some", "repo").delete()
        assert not repository_exists(repo)

    finally:
        local_engine_empty.initialize()
        local_engine_empty.commit()
