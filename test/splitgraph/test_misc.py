from datetime import datetime as dt
from unittest.mock import patch

import pytest
from psycopg2.errors import CheckViolation
from splitgraph.cloud.models import ExternalTableRequest
from splitgraph.core.common import Tracer, adapt, coerce_val_to_json
from splitgraph.core.engine import lookup_repository
from splitgraph.core.metadata_manager import Object
from splitgraph.core.output import parse_dt
from splitgraph.core.repository import Repository
from splitgraph.core.types import (
    TableColumn,
    dict_to_table_schema_params,
    table_schema_params_to_dict,
)
from splitgraph.engine.postgres.engine import API_MAX_QUERY_LENGTH
from splitgraph.exceptions import RepositoryNotFoundError
from splitgraph.hooks.s3 import get_object_upload_urls


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
                    created=datetime.utcnow(),
                    insertion_hash="0" * 64,
                    deletion_hash="0" * 64,
                    object_index={},
                    rows_inserted=10,
                    rows_deleted=2,
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
                    created=datetime.utcnow(),
                    insertion_hash="0" * 64,
                    deletion_hash="0" * 64,
                    object_index={},
                    rows_inserted=10,
                    rows_deleted=2,
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
                    created=datetime.utcnow(),
                    insertion_hash="0" * 64,
                    deletion_hash="0" * 64,
                    object_index={},
                    rows_inserted=10,
                    rows_deleted=2,
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
                    created=datetime.utcnow(),
                    insertion_hash="broken",
                    deletion_hash="0" * 64,
                    object_index={},
                    rows_inserted=10,
                    rows_deleted=2,
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
                    created=datetime.utcnow(),
                    insertion_hash="0" * 64,
                    deletion_hash="broken",
                    object_index={},
                    rows_inserted=10,
                    rows_deleted=2,
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
                created=datetime.utcnow(),
                insertion_hash="0" * 64,
                deletion_hash="0" * 64,
                object_index={},
                rows_inserted=10,
                rows_deleted=2,
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


@pytest.mark.registry
def test_large_api_calls(unprivileged_pg_repo):
    # Test query chunking for API calls that exceed length/vararg limits

    # Make a fake object with 64KB of bloom index data (doesn't fit into the min query size
    # at all)
    fake_object = Object(
        object_id="o%062d" % 0,
        format="FRAG",
        namespace=unprivileged_pg_repo.namespace,
        size=42,
        created=datetime.utcnow(),
        insertion_hash="0" * 64,
        deletion_hash="0" * 64,
        object_index={"bloom": [42, "A" * API_MAX_QUERY_LENGTH]},
        rows_inserted=10,
        rows_deleted=2,
    )

    with pytest.raises(ValueError) as e:
        unprivileged_pg_repo.objects.register_objects(
            [fake_object], namespace=unprivileged_pg_repo.namespace
        )
    assert "exceeds maximum query size" in str(e.value)

    # Make a bunch of fake objects and try registering them
    # Each object has 1KB of bloom index data (+ a few bytes of misc metadata) and we're
    # making 1000 objects -- check that queries get chunked up.
    objects = [
        Object(
            object_id="o%062d" % i,
            format="FRAG",
            namespace=unprivileged_pg_repo.namespace,
            size=42,
            created=datetime.utcnow(),
            insertion_hash="0" * 64,
            deletion_hash="0" * 64,
            object_index={"bloom": [42, "A" * 1024]},
            rows_inserted=42,
            rows_deleted=0,
        )
        for i in range(2000)
    ]
    all_ids = [o.object_id for o in objects]
    # Check objects don't exist (query should also get chunked up) and register them
    new_objects = unprivileged_pg_repo.objects.get_new_objects(all_ids)
    assert new_objects == all_ids
    unprivileged_pg_repo.objects.register_objects(objects, namespace=unprivileged_pg_repo.namespace)

    # Get presigned URLs for these objects
    urls = get_object_upload_urls(unprivileged_pg_repo.engine, all_ids)
    assert len(urls) == 2000

    # Get our objects back
    meta = unprivileged_pg_repo.objects.get_object_meta(all_ids)
    assert len(meta) == 2000

    # Now make an image with a lot of objects
    image_hash = "0" * 63 + "1"
    unprivileged_pg_repo.images.add(parent_id=None, image=image_hash)

    # Two tables to test that register_tables chunks correctly with multiple tables
    unprivileged_pg_repo.objects.register_tables(
        unprivileged_pg_repo,
        [
            (image_hash, "small_table", [(1, "key", "integer", True)], [all_ids[0]]),
            (
                image_hash,
                "table",
                [(1, "key", "integer", True)],
                all_ids,
            ),
        ],
    )

    # Get table back and check that it has the same objects (multiple add_table calls
    # add new objects to the table)
    table = unprivileged_pg_repo.images[image_hash].get_table("table")
    assert table.objects == all_ids
    small_table = unprivileged_pg_repo.images[image_hash].get_table("small_table")
    assert small_table.objects == [all_ids[0]]


def test_pg_type_adapt():
    assert adapt(None, "character varying") is None
    assert adapt("test", "character varying") == "test"
    assert adapt("42", "bigint") == 42
    assert adapt(42, "bigint") == 42
    assert adapt("2.0", "numeric") == 2.0


def test_val_to_json():
    assert coerce_val_to_json(datetime(2010, 1, 1)) == "2010-01-01 00:00:00"
    assert coerce_val_to_json([1, 2, datetime(2010, 1, 1)]) == [1, 2, "2010-01-01 00:00:00"]
    assert coerce_val_to_json({"one": 1, "two": 2, "datetime": datetime(2010, 1, 1)}) == {
        "one": 1,
        "two": 2,
        "datetime": "2010-01-01 00:00:00",
    }


def test_parse_dt():
    assert parse_dt("2020-01-01 12:00:01.123456") == datetime(2020, 1, 1, 12, 0, 1, 123456)
    assert parse_dt("2020-01-01T12:34:56") == datetime(2020, 1, 1, 12, 34, 56)

    with pytest.raises(ValueError):
        parse_dt("not a dt")


def test_table_schema_params_to_dict():
    assert table_schema_params_to_dict(
        {
            "fruits": (
                [
                    TableColumn(
                        ordinal=1, name="fruit_id", pg_type="integer", is_pk=False, comment=None
                    ),
                    TableColumn(
                        ordinal=2,
                        name="name",
                        pg_type="character varying",
                        is_pk=False,
                        comment=None,
                    ),
                ],
                {"key": "value"},
            ),
            "vegetables": (
                [
                    TableColumn(
                        ordinal=1, name="vegetable_id", pg_type="integer", is_pk=False, comment=None
                    ),
                    TableColumn(
                        ordinal=2,
                        name="name",
                        pg_type="character varying",
                        is_pk=False,
                        comment=None,
                    ),
                ],
                {"key": "value", "key_2": ["this", "is", "an", "array"]},
            ),
        }
    ) == {
        "fruits": {
            "schema": {"fruit_id": "integer", "name": "character varying"},
            "options": {"key": "value"},
        },
        "vegetables": {
            "schema": {"name": "character varying", "vegetable_id": "integer"},
            "options": {"key": "value", "key_2": ["this", "is", "an", "array"]},
        },
    }


def test_dict_to_table_schema_params():
    assert dict_to_table_schema_params(
        {
            k: ExternalTableRequest.parse_obj(v)
            for k, v in {
                "fruits": {
                    "schema": {"fruit_id": "integer", "name": "character varying"},
                    "options": {"key": "value"},
                },
                "vegetables": {
                    "schema": {"name": "character varying", "vegetable_id": "integer"},
                    "options": {"key": "value"},
                },
            }.items()
        }
    ) == {
        "fruits": (
            [
                TableColumn(
                    ordinal=1, name="fruit_id", pg_type="integer", is_pk=False, comment=None
                ),
                TableColumn(
                    ordinal=2, name="name", pg_type="character varying", is_pk=False, comment=None
                ),
            ],
            {"key": "value"},
        ),
        "vegetables": (
            [
                TableColumn(
                    ordinal=1, name="name", pg_type="character varying", is_pk=False, comment=None
                ),
                TableColumn(
                    ordinal=2, name="vegetable_id", pg_type="integer", is_pk=False, comment=None
                ),
            ],
            {"key": "value"},
        ),
    }
