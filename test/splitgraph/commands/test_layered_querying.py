import json
from datetime import datetime as dt

import pytest

from splitgraph import Repository
from splitgraph.core import clone
from splitgraph.core._common import META_TABLES
from splitgraph.engine import ResultShape
from test.splitgraph.conftest import _assert_cache_occupancy, OUTPUT


def prepare_lq_repo(repo, commit_after_every, include_pk, snap_only=False):
    OPS = [
        "INSERT INTO fruits VALUES (3, 'mayonnaise')",
        "DELETE FROM fruits WHERE name = 'apple'",
        "DELETE FROM vegetables WHERE vegetable_id = 1;INSERT INTO vegetables VALUES (3, 'celery')",
        "UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2",
    ]

    repo.run_sql("ALTER TABLE fruits ADD COLUMN number NUMERIC DEFAULT 1")
    repo.run_sql("ALTER TABLE fruits ADD COLUMN timestamp TIMESTAMP DEFAULT '2019-01-01T12:00:00'")
    if include_pk:
        repo.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
        repo.run_sql("ALTER TABLE vegetables ADD PRIMARY KEY (vegetable_id)")
        repo.commit()

    for o in OPS:
        repo.run_sql(o)
        print(o)

        if commit_after_every:
            repo.commit(snap_only=snap_only)
    if not commit_after_every:
        repo.commit(snap_only=snap_only)


_DT = dt(2019, 1, 1, 12)


@pytest.mark.parametrize(
    "test_case",
    [
        ("SELECT * FROM fruits WHERE fruit_id = 3", [(3, "mayonnaise", 1, _DT)]),
        ("SELECT * FROM fruits WHERE fruit_id = 2", [(2, "guitar", 1, _DT)]),
        ("SELECT * FROM vegetables WHERE vegetable_id = 1", []),
        ("SELECT * FROM fruits WHERE fruit_id = 1", []),
        # Test quals on other types
        (
            "SELECT * FROM fruits WHERE fruit_id = 3 AND timestamp > '2018-01-01T00:00:00'",
            [(3, "mayonnaise", 1, _DT)],
        ),
        # EQ on string
        ("SELECT * FROM fruits WHERE name = 'guitar'", [(2, "guitar", 1, _DT)]),
        # IN ( converted to =ANY(array([...]))
        (
            "SELECT * FROM fruits WHERE name IN ('guitar', 'mayonnaise') ORDER BY fruit_id",
            [(2, "guitar", 1, _DT), (3, "mayonnaise", 1, _DT)],
        ),
        # LIKE (operator ~~)
        ("SELECT * FROM fruits WHERE name LIKE '%uitar'", [(2, "guitar", 1, _DT)]),
        # Join between two FDWs
        (
            "SELECT * FROM fruits JOIN vegetables ON fruits.fruit_id = vegetables.vegetable_id",
            [(2, "guitar", 1, _DT, 2, "carrot"), (3, "mayonnaise", 1, _DT, 3, "celery")],
        ),
        # Expression in terms of another column
        ("SELECT * FROM fruits WHERE fruit_id = number + 1 ", [(2, "guitar", 1, _DT)]),
    ],
)
def test_layered_querying(pg_repo_local, test_case):
    # Future: move the LQ tests to be local (instantiate the FDW with some mocks and send the same query requests)
    # since it's much easier to test them like that.

    # Most of these tests are only interesting for where there are multiple fragments, so we have PKs on tables
    # and store them as deltas.

    prepare_lq_repo(pg_repo_local, commit_after_every=True, include_pk=True)

    # Discard the actual materialized table and query everything via FDW
    new_head = pg_repo_local.head
    new_head.checkout(layered=True)

    query, expected = test_case
    print("Query: %s, expected: %r" % test_case)
    assert pg_repo_local.run_sql(query) == expected


def test_layered_querying_against_single_fragment(pg_repo_local):
    # Test the case where the query is satisfied by a single fragment.
    prepare_lq_repo(pg_repo_local, snap_only=True, commit_after_every=False, include_pk=True)
    new_head = pg_repo_local.head
    new_head.checkout(layered=True)

    assert pg_repo_local.run_sql(
        "SELECT * FROM fruits WHERE name IN ('guitar', 'mayonnaise') ORDER BY fruit_id"
    ) == [(2, "guitar", 1, _DT), (3, "mayonnaise", 1, _DT)]


def test_layered_querying_type_conversion(pg_repo_local):
    # For type bigint, Multicorn for some reason converts quals to be strings. Test we can handle that.
    prepare_lq_repo(pg_repo_local, commit_after_every=False, include_pk=True)
    pg_repo_local.run_sql("ALTER TABLE fruits ALTER COLUMN fruit_id TYPE bigint")
    pg_repo_local.commit()
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (4, 'kumquat', 42, '2018-01-02T03:04:05')")
    new_head = pg_repo_local.commit()
    new_head.checkout(layered=True)

    # Make sure ANY works on integers (not converted to strings)
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id IN (3, 4)") == [
        (3, "mayonnaise", 1, _DT),
        (4, "kumquat", 42, dt(2018, 1, 2, 3, 4, 5)),
    ]


def test_layered_querying_json(local_engine_empty):
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value JSONB)")
    OUTPUT.run_sql("INSERT INTO test VALUES (1, %s)", (json.dumps({"a": 1, "b": 2.5}),))
    OUTPUT.commit()
    OUTPUT.run_sql(
        "INSERT INTO test VALUES (2, %s)", (json.dumps({"a": "one", "b": "two point five"}),)
    )
    head = OUTPUT.commit()
    OUTPUT.uncheckout()
    head.checkout(layered=True)

    assert OUTPUT.run_sql("SELECT * FROM test ORDER BY key") == [
        (1, {"a": 1, "b": 2.5}),
        (2, {"a": "one", "b": "two point five"}),
    ]


def _test_lazy_lq_checkout(pg_repo_local):
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 0
    # Do a lazy LQ checkout -- no objects should be downloaded yet
    pg_repo_local.images["latest"].checkout(layered=True)
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 0
    # Actual LQ still downloads the objects, but one by one.
    # Hit fruits -- 2 objects should be downloaded (one fragment and a patch on top of it but not the very first one)
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id = 2") == [
        (2, "guitar", 1, _DT)
    ]
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 2
    # Hit vegetables -- 2 more objects should be downloaded
    assert pg_repo_local.run_sql("SELECT * FROM vegetables WHERE vegetable_id = 1") == []
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 4


def test_lq_remote(local_engine_empty, pg_repo_remote):
    # Test layered querying works when we initialize it on a cloned repo that doesn't have any
    # cached objects (all are on the remote).

    # 1 patch on top of fruits, 1 patch on top of vegetables
    prepare_lq_repo(pg_repo_remote, commit_after_every=False, include_pk=True)
    pg_repo_local = clone(pg_repo_remote, download_all=False)
    _test_lazy_lq_checkout(pg_repo_local)


@pytest.mark.registry
def test_lq_external(local_engine_empty, unprivileged_pg_repo, pg_repo_remote_registry):
    # Test layered querying works when we initialize it on a cloned repo that doesn't have any
    # cached objects (all are on S3 or other external location).

    pg_repo_local = clone(unprivileged_pg_repo)
    pg_repo_local.images["latest"].checkout()
    prepare_lq_repo(pg_repo_local, commit_after_every=False, include_pk=True)

    # Setup: upstream has the same repository as in the previous test but with no cached objects (all are external).
    # In addition, we check that LQ works against an unprivileged upstream (where we don't actually have
    # admin access).
    pg_repo_local.push(unprivileged_pg_repo, handler="S3", handler_options={})
    pg_repo_local.delete()
    pg_repo_local.objects.cleanup()

    assert len(pg_repo_local.objects.get_all_objects()) == 0
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 0
    assert len(pg_repo_remote_registry.objects.get_all_objects()) == 6
    assert len(pg_repo_remote_registry.objects.get_downloaded_objects()) == 0

    # Proceed as per the previous test
    pg_repo_local = clone(unprivileged_pg_repo, download_all=False)
    _test_lazy_lq_checkout(pg_repo_local)


def _prepare_fully_remote_repo(local_engine_empty, pg_repo_remote_registry):
    # Setup: same as external, with an extra patch on top of the fruits table.
    pg_repo_local = clone(pg_repo_remote_registry)
    pg_repo_local.images["latest"].checkout()
    prepare_lq_repo(pg_repo_local, commit_after_every=True, include_pk=True)
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (4, 'kumquat')")
    pg_repo_local.commit()
    pg_repo_local.push(handler="S3", handler_options={})
    pg_repo_local.delete()
    pg_repo_local.objects.cleanup()
    pg_repo_local.commit_engines()


@pytest.mark.parametrize(
    "test_case",
    [
        # Each test case is a: query, expected result, mask of which objects were downloaded
        # Test single PK qual
        (
            "SELECT * FROM fruits WHERE fruit_id = 4",
            [(4, "kumquat", 1, _DT)],
            (False, False, False, False, True),
        ),
        # Test range fetches 2 objects
        (
            "SELECT * FROM fruits WHERE fruit_id >= 3",
            [(3, "mayonnaise", 1, _DT), (4, "kumquat", 1, _DT)],
            (False, True, False, False, True),
        ),
        # Test the upsert fetches the original fragment as well as one that overwrites it
        (
            "SELECT * FROM fruits WHERE fruit_id = 2",
            [(2, "guitar", 1, _DT)],
            (True, False, False, True, False),
        ),
        # Test NULLs don't break anything (even though we still look at all objects)
        ("SELECT * FROM fruits WHERE name IS NULL", [], (True, True, True, True, True)),
        # Same but also add a filter on the string column to exclude 'guitar'.
        # Make sure the chunk that updates 'orange' into 'guitar' is still fetched
        # since it overwrites the old value (even though the updated value doesn't match the qual any more)
        (
            "SELECT * FROM fruits WHERE fruit_id = 2 AND name > 'guitar'",
            [],
            (True, False, False, True, False),
        ),
        # Similar here: the chunk that deletes 'apple' is supposed to have 'apple' included in its index
        # and fetched as well.
        ("SELECT * FROM fruits WHERE name = 'apple'", [], (True, False, True, False, False)),
    ],
)
@pytest.mark.registry
def test_lq_qual_filtering(local_engine_empty, unprivileged_pg_repo, test_case):
    # Test that LQ prunes the object list based on quals
    # We can't really see that directly, so we check to see which objects it tries to download.
    _prepare_fully_remote_repo(local_engine_empty, unprivileged_pg_repo)

    pg_repo_local = clone(unprivileged_pg_repo, download_all=False)
    pg_repo_local.images["latest"].checkout(layered=True)
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 0

    query, expected, object_mask = test_case
    required_objects = list(
        pg_repo_local.objects.get_all_required_objects(
            pg_repo_local.head.get_table("fruits").objects
        )
    )
    assert len(required_objects) == 5
    assert required_objects == [
        # Initial fragment
        "of22f20503d3bf17c7449b545d68ebcee887ed70089f0342c4bff38862c0dc5",
        # DEL (1, apple)
        "oa32db57247f1d5cea7c0ac7df3cf0a74fe552cf9fd07078612c774e8f3472f",
        # UPS (2, guitar), replaces (2, orange)
        "ofb935b4decb6062665d8d583d1c266f88dfddad8705d6a33eff1aa8ac1e767",
        # INS (3, mayonnaise)
        "occfcd55402d9ca3d3d7fa18dd56227d56df4151888a9518c9103b3bac0ee8c",
        # INS (4, kumquat)
        "o75dd055ad2465eb1c3f4e03c6f772c48d87029ef6f141fd4cf3d198e5b247f",
    ]

    expected_objects = [o for o, m in zip(required_objects, object_mask) if m]

    assert pg_repo_local.run_sql(query) == expected
    used_objects = pg_repo_local.objects.get_downloaded_objects()
    assert set(expected_objects) == set(used_objects)


@pytest.mark.registry
def test_lq_single_non_snap_object(local_engine_empty, unprivileged_pg_repo):
    # The object produced by
    # "DELETE FROM vegetables WHERE vegetable_id = 1;INSERT INTO vegetables VALUES (3, 'celery')"
    # has a deletion and an insertion. Check that an LQ that only uses that object
    # doesn't return the extra upserted/deleted flag column.

    _prepare_fully_remote_repo(local_engine_empty, unprivileged_pg_repo)

    pg_repo_local = clone(unprivileged_pg_repo, download_all=False)
    pg_repo_local.images["latest"].checkout(layered=True)

    assert pg_repo_local.run_sql(
        "SELECT * FROM vegetables WHERE vegetable_id = 3 AND name = 'celery'"
    ) == [(3, "celery")]
    used_objects = pg_repo_local.objects.get_downloaded_objects()
    assert len(used_objects) == 1


@pytest.mark.parametrize(
    "test_case",
    [
        # Normal quals
        ([[("fruit_id", "=", "2")]], [{"name": "guitar", "timestamp": _DT}]),
        # No quals
        (
            [],
            [
                {"name": "mayonnaise", "timestamp": dt(2019, 1, 1, 12, 0)},
                {"name": "guitar", "timestamp": dt(2019, 1, 1, 12, 0)},
            ],
        ),
        # One fragment hit
        ([[("fruit_id", "=", "3")]], [{"name": "mayonnaise", "timestamp": dt(2019, 1, 1, 12, 0)}]),
        # No fragments hit
        ([[("fruit_id", "=", "42")]], []),
    ],
)
def test_direct_table_lq(pg_repo_local, test_case):
    # Test LQ using the Table.query() call instead of the FDW
    prepare_lq_repo(pg_repo_local, commit_after_every=True, include_pk=True)

    new_head = pg_repo_local.head
    table = new_head.get_table("fruits")

    quals, expected = test_case
    assert list(table.query(columns=["name", "timestamp"], quals=quals)) == expected


@pytest.mark.registry
def test_multiengine_flow(local_engine_empty, unprivileged_pg_repo, pg_repo_remote_registry):
    # Test querying by using the remote engine as a metadata store and the local engine as an object store.
    _prepare_fully_remote_repo(local_engine_empty, unprivileged_pg_repo)
    pg_repo_local = Repository.from_template(unprivileged_pg_repo, object_engine=local_engine_empty)

    # Checkout currently requires the engine connection to be privileged
    # (since it does manage_audit_triggers()) -- so we bypass all bookkeeping and call the
    # actual LQ routine directly.
    local_engine_empty.create_schema(pg_repo_local.to_schema())
    pg_repo_local.images["latest"]._lq_checkout()

    # Take one of the test cases we ran in test_lq_qual_filtering that exercises index lookups,
    # LQs, object downloads and make sure that the correct engines are used
    result = pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id >= 3")
    assert result == [(3, "mayonnaise", 1, _DT), (4, "kumquat", 1, _DT)]

    # Test cache occupancy calculations work only using the object engine
    _assert_cache_occupancy(pg_repo_local.objects, 2)

    # 2 objects downloaded from S3 to satisfy the query -- on the local engine
    assert (
        local_engine_empty.run_sql(
            "SELECT COUNT(1) FROM splitgraph_meta.object_cache_status",
            return_shape=ResultShape.ONE_ONE,
        )
        == 2
    )
    assert (
        len(set(local_engine_empty.get_all_tables("splitgraph_meta")).difference(set(META_TABLES)))
        == 2
    )

    # Test the local engine doesn't actually have any metadata stored on it.
    for table in META_TABLES:
        if table not in ("object_cache_status", "object_cache_occupancy"):
            assert (
                local_engine_empty.run_sql(
                    "SELECT COUNT(1) FROM splitgraph_meta." + table,
                    return_shape=ResultShape.ONE_ONE,
                )
                == 0
            )

    assert len(pg_repo_remote_registry.objects.get_downloaded_objects()) == 0
    assert (
        len(
            set(pg_repo_remote_registry.engine.get_all_tables("splitgraph_meta")).difference(
                set(META_TABLES)
            )
        )
        == 0
    )
