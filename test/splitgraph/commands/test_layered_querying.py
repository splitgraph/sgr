import json
import logging
import time
from datetime import datetime as dt
from unittest import mock
from unittest.mock import call

import pytest
from test.splitgraph.conftest import _assert_cache_occupancy, OUTPUT, prepare_lq_repo

from splitgraph.config import SPLITGRAPH_META_SCHEMA, CONFIG
from splitgraph.core.common import META_TABLES
from splitgraph.core.fragment_manager import get_chunk_groups
from splitgraph.core.indexing.range import extract_min_max_pks
from splitgraph.core.object_manager import ObjectManager
from splitgraph.core.repository import clone, Repository
from splitgraph.core.table import _generate_select_query
from splitgraph.engine import ResultShape, _prepare_engine_config
from splitgraph.engine.postgres.engine import PostgresEngine
from splitgraph.exceptions import ObjectNotFoundError

_DT = dt(2019, 1, 1, 12)


def _assert_dict_list_equal(left, right):
    """Check list-of-dicts equality without regard for order"""
    assert sorted(left, key=str) == sorted(right, key=str)


class TestLayeredQuerying:
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
            # Fetching subsets of columns
            ("SELECT name, number FROM fruits WHERE fruit_id = number + 1 ", [("guitar", 1)]),
            # Different order
            (
                "SELECT timestamp, number, name FROM fruits WHERE fruit_id = number + 1 ",
                [(_DT, 1, "guitar")],
            ),
            # Duplicate columns
            (
                "SELECT number, number, name FROM fruits WHERE fruit_id = number + 1 ",
                [(1, 1, "guitar")],
            ),
        ],
    )
    def test_layered_querying(self, lq_test_repo, test_case):
        # Future: move the LQ tests to be local (instantiate the FDW with some mocks and send the same query requests)
        # since it's much easier to test them like that.

        query, expected = test_case
        print("Query: %s, expected: %r" % test_case)
        assert sorted(lq_test_repo.run_sql(query)) == sorted(expected)
        lq_test_repo.engine.rollback()

    @pytest.mark.parametrize(
        "test_case",
        [
            # Fetching subsets of columns
            ("SELECT name, number FROM fruits WHERE fruit_id = number + 1 ", [("guitar", 1)]),
            # Different order
            (
                "SELECT timestamp, number, name FROM fruits WHERE fruit_id = number + 1 ",
                [(_DT, 1, "guitar")],
            ),
            # Duplicate columns
            (
                "SELECT number, number, name FROM fruits WHERE fruit_id = number + 1 ",
                [(1, 1, "guitar")],
            ),
        ],
    )
    def test_layered_querying_query_schema(self, lq_test_repo, test_case):
        query, expected = test_case
        print("Query: %s, expected: %r" % test_case)
        with lq_test_repo.head.query_schema() as s:
            assert sorted(lq_test_repo.engine.run_sql_in(s, query)) == sorted(expected)

    def test_layered_querying_mount_comments(self, lq_test_repo):
        with lq_test_repo.head.query_schema() as s:
            assert (
                lq_test_repo.engine.run_sql_in(
                    s,
                    "SELECT col_description('fruits'::regclass, 2)",
                    return_shape=ResultShape.ONE_ONE,
                )
                == "Name of the fruit"
            )

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
            (
                [[("fruit_id", "=", "3")]],
                [{"name": "mayonnaise", "timestamp": dt(2019, 1, 1, 12, 0)}],
            ),
            # No fragments hit
            ([[("fruit_id", "=", "42")]], []),
        ],
    )
    def test_direct_table_lq(self, lq_test_repo, test_case):
        # Test LQ using the Table.query() call instead of the FDW
        table = lq_test_repo.head.get_table("fruits")

        quals, expected = test_case
        actual = table.query(columns=["name", "timestamp"], quals=quals)
        _assert_dict_list_equal(actual, expected)

    def test_direct_table_lq_query_plan_cache(self, lq_test_repo):
        table = lq_test_repo.head.get_table("fruits")

        quals, expected = ([[("fruit_id", "=", "2")]], [{"name": "guitar", "timestamp": _DT}])

        # Check "query plan" is reused and the table doesn't run qual filtering again
        with mock.patch.object(
            ObjectManager, "filter_fragments", wraps=table.repository.objects.filter_fragments
        ) as fo:
            table.query(columns=["name", "timestamp"], quals=quals)
            assert fo.call_count == 1
            table.query(columns=["name", "timestamp"], quals=quals)
            assert fo.call_count == 1

        query_plan = table.get_query_plan(quals=quals, columns=["name", "timestamp"])
        assert query_plan.estimated_rows == 2
        assert len(query_plan.required_objects) == 4
        assert len(query_plan.filtered_objects) == 2


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
    assert pg_repo_local.run_sql(
        "SELECT * FROM fruits WHERE fruit_id IN (3, 4) ORDER BY fruit_id"
    ) == [(3, "mayonnaise", 1, _DT), (4, "kumquat", 42, dt(2018, 1, 2, 3, 4, 5))]


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
def test_lq_external(
    local_engine_empty, unprivileged_pg_repo, pg_repo_remote_registry, clean_minio
):
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
            "SELECT * FROM fruits WHERE fruit_id >= 3 ORDER BY fruit_id",
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
def test_lq_qual_filtering(local_engine_empty, unprivileged_pg_repo, clean_minio, test_case):
    # Test that LQ prunes the object list based on quals
    # We can't really see that directly, so we check to see which objects it tries to download.
    _prepare_fully_remote_repo(local_engine_empty, unprivileged_pg_repo)

    pg_repo_local = clone(unprivileged_pg_repo, download_all=False)
    pg_repo_local.images["latest"].checkout(layered=True)
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 0

    query, expected, object_mask = test_case
    required_objects = pg_repo_local.head.get_table("fruits").objects

    assert len(required_objects) == 5
    assert required_objects == [
        # Initial fragment
        "of22f20503d3bf17c7449b545d68ebcee887ed70089f0342c4bff38862c0dc5",
        # INS (3, mayonnaise)
        "of0fb43e477311f82aa30055be303ff00599dfe155d737def0d00f06e07228b",
        # DEL (1, apple)
        "o23fe42d48d7545596d0fea1c48bcf7d64bde574d437c77cc5bb611e5f8849d",
        # UPS (2, guitar), replaces (2, orange)
        "o3f81f6c40ecc3366d691a2ce45f41f6f180053020607cbd0873baf0c4447dc",
        # INS (4, kumquat)
        "oc27ee277aff108525a2df043d9efdaa1c3e26a4949a6cf6b53ee0c889c8559",
    ]

    expected_objects = [o for o, m in zip(required_objects, object_mask) if m]

    assert pg_repo_local.run_sql(query) == expected
    used_objects = pg_repo_local.objects.get_downloaded_objects()
    assert set(expected_objects) == set(used_objects)


@pytest.mark.registry
def test_lq_single_non_snap_object(local_engine_empty, unprivileged_pg_repo, clean_minio):
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


@pytest.mark.registry
def test_multiengine_flow(
    local_engine_empty, unprivileged_pg_repo, pg_repo_remote_registry, clean_minio
):
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
    result = pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id >= 3 ORDER BY fruit_id")
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
        if table not in ("object_cache_status", "object_cache_occupancy", "version"):
            assert (
                local_engine_empty.run_sql(
                    "SELECT COUNT(1) FROM splitgraph_meta." + table,
                    return_shape=ResultShape.ONE_ONE,
                )
                == 0
            )

    # remote engine untouched
    assert (
        pg_repo_remote_registry.engine.run_sql(
            "SELECT COUNT(1) FROM splitgraph_meta.object_cache_status",
            return_shape=ResultShape.ONE_ONE,
        )
        == 0
    )


def _get_chunk_groups(table):
    pks = [c.name for c in table.table_schema if c.is_pk]
    pk_types = [c.pg_type for c in table.table_schema if c.is_pk]
    chunk_boundaries = extract_min_max_pks(
        table.repository.objects.object_engine, table.objects, pks, pk_types
    )
    return get_chunk_groups(
        [(o, min_max[0], min_max[1]) for o, min_max in zip(table.objects, chunk_boundaries)]
    )


def test_disjoint_table_lq_one_singleton(pg_repo_local):
    # Test querying tables that have multiple single chunks that don't overlap each other.
    # Those must be queried directly without being applied to a staging area.

    prepare_lq_repo(pg_repo_local, commit_after_every=True, include_pk=True)
    fruits = pg_repo_local.images["latest"].get_table("fruits")

    # Quick sanity checks/assertions to show which chunks in the table overlap which.
    assert _get_chunk_groups(fruits) == [
        [
            # Group 1: original two rows (PKs 1 and 2)...
            ("of22f20503d3bf17c7449b545d68ebcee887ed70089f0342c4bff38862c0dc5", (1,), (2,)),
            # ...then deletion of 'apple' (PK 1)
            ("o23fe42d48d7545596d0fea1c48bcf7d64bde574d437c77cc5bb611e5f8849d", (1,), (1,)),
            # ...then update PK 2 to 'guitar'
            ("o3f81f6c40ecc3366d691a2ce45f41f6f180053020607cbd0873baf0c4447dc", (2,), (2,)),
        ],
        # Group 2: even though this insertion happened first, it's separated out
        # as it can be applied independently.
        [("of0fb43e477311f82aa30055be303ff00599dfe155d737def0d00f06e07228b", (3,), (3,))],
    ]

    # Run query that only touches the chunk with pk=3: since we skip over the chunks in the first group,
    # we aren't supposed to call apply_fragments and just query the oa32... chunk directly.
    with mock.patch.object(
        PostgresEngine, "apply_fragments", wraps=pg_repo_local.engine.apply_fragments
    ) as apply_fragments:
        assert list(
            fruits.query(columns=["fruit_id", "name"], quals=[[("fruit_id", "=", "3")]])
        ) == [{"fruit_id": 3, "name": "mayonnaise"}]
        assert apply_fragments.call_count == 0


def test_disjoint_table_lq_indirect(pg_repo_local):
    # Test querying tables indirectly (returning some tables)
    prepare_lq_repo(pg_repo_local, commit_after_every=True, include_pk=True)
    fruits = pg_repo_local.images["latest"].get_table("fruits")

    result, callback, _ = fruits.query_indirect(
        columns=["fruit_id", "name"], quals=[[("fruit_id", "=", "3")]]
    )

    # Check that we get a list of chunks to query and a callback to release the results
    assert list(result) == [
        b'"splitgraph_meta"."of0fb43e477311f82aa30055be303ff00599dfe155d737def0d00f06e07228b"'
    ]
    assert len(callback) == 1


def test_disjoint_table_lq_two_singletons(pg_repo_local):
    # Add another two rows to the table with PKs 4 and 5
    prepare_lq_repo(pg_repo_local, commit_after_every=True, include_pk=True)
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (4, 'fruit_4'), (5, 'fruit_5')")
    fruits = pg_repo_local.commit().get_table("fruits")
    # The new fragment lands in a separate group
    assert _get_chunk_groups(fruits) == [
        [
            ("of22f20503d3bf17c7449b545d68ebcee887ed70089f0342c4bff38862c0dc5", (1,), (2,)),
            ("o23fe42d48d7545596d0fea1c48bcf7d64bde574d437c77cc5bb611e5f8849d", (1,), (1,)),
            ("o3f81f6c40ecc3366d691a2ce45f41f6f180053020607cbd0873baf0c4447dc", (2,), (2,)),
        ],
        [("of0fb43e477311f82aa30055be303ff00599dfe155d737def0d00f06e07228b", (3,), (3,))],
        [("oaa6d009e485bfa91aec4ab6b0ed1ebcd67055f6a3420d29f26446b034f41cc", (4,), (5,))],
    ]

    # Query hitting PKs 3, 4 and 5: they hit single chunks that don't depend on anything,
    # so we still shouldn't be applying fragments.
    with mock.patch.object(
        PostgresEngine, "apply_fragments", wraps=pg_repo_local.engine.apply_fragments
    ) as apply_fragments:

        with mock.patch(
            "splitgraph.core.table._generate_select_query", side_effect=_generate_select_query
        ) as _gsc:
            assert list(
                fruits.query(columns=["fruit_id", "name"], quals=[[("fruit_id", ">=", "3")]])
            ) == [
                {"fruit_id": 3, "name": "mayonnaise"},
                {"fruit_id": 4, "name": "fruit_4"},
                {"fruit_id": 5, "name": "fruit_5"},
            ]
            assert apply_fragments.call_count == 0

            # Check that we generated two SELECT queries
            assert _gsc.mock_calls == [
                call(
                    mock.ANY,
                    b'"splitgraph_meta"."of0fb43e477311f82aa30055be303ff00599dfe155d737def0d00f06e07228b"',
                    ["fruit_id", "name"],
                    mock.ANY,
                    ("3",),
                ),
                call(
                    mock.ANY,
                    b'"splitgraph_meta"."oaa6d009e485bfa91aec4ab6b0ed1ebcd67055f6a3420d29f26446b034f41cc"',
                    ["fruit_id", "name"],
                    mock.ANY,
                    ("3",),
                ),
            ]


def test_disjoint_table_lq_two_singletons_one_overwritten(pg_repo_local):
    # Add another two rows to the table with PKs 4 and 5
    prepare_lq_repo(pg_repo_local, commit_after_every=True, include_pk=True)
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (4, 'fruit_4'), (5, 'fruit_5')")
    pg_repo_local.commit()
    pg_repo_local.run_sql("UPDATE fruits SET name = 'fruit_5_updated' WHERE fruit_id = 5")
    fruits = pg_repo_local.commit().get_table("fruits")

    assert _get_chunk_groups(fruits) == [
        [
            ("of22f20503d3bf17c7449b545d68ebcee887ed70089f0342c4bff38862c0dc5", (1,), (2,)),
            ("o23fe42d48d7545596d0fea1c48bcf7d64bde574d437c77cc5bb611e5f8849d", (1,), (1,)),
            ("o3f81f6c40ecc3366d691a2ce45f41f6f180053020607cbd0873baf0c4447dc", (2,), (2,)),
        ],
        [("of0fb43e477311f82aa30055be303ff00599dfe155d737def0d00f06e07228b", (3,), (3,))],
        # The pk=5 update has to be added to the last chunk group, making it a non-singleton
        [
            ("oaa6d009e485bfa91aec4ab6b0ed1ebcd67055f6a3420d29f26446b034f41cc", (4,), (5,)),
            ("o15a420721b04e9749761b5368628cb15593cb8cfdcc547107b98eddda5031d", (5,), (5,)),
        ],
    ]

    with mock.patch.object(
        PostgresEngine, "apply_fragments", wraps=pg_repo_local.engine.apply_fragments
    ) as apply_fragments:
        with mock.patch(
            "splitgraph.core.table._generate_select_query", side_effect=_generate_select_query
        ) as _gsc:
            assert list(
                fruits.query(columns=["fruit_id", "name"], quals=[[("fruit_id", ">=", "3")]])
            ) == [
                {"fruit_id": 3, "name": "mayonnaise"},
                {"fruit_id": 4, "name": "fruit_4"},
                {"fruit_id": 5, "name": "fruit_5_updated"},
            ]

            # This time we had to apply the fragments in the final group (since there were two of them)
            apply_fragments.assert_called_once_with(
                [
                    (
                        "splitgraph_meta",
                        "oaa6d009e485bfa91aec4ab6b0ed1ebcd67055f6a3420d29f26446b034f41cc",
                    ),
                    (
                        "splitgraph_meta",
                        "o15a420721b04e9749761b5368628cb15593cb8cfdcc547107b98eddda5031d",
                    ),
                ],
                SPLITGRAPH_META_SCHEMA,
                mock.ANY,
                extra_qual_args=("3",),
                extra_quals=mock.ANY,
                schema_spec=mock.ANY,
            )

            # Two calls to _generate_select_queries -- one to directly query the pk=3 chunk...
            assert _gsc.call_args_list == [
                mock.call(
                    pg_repo_local.engine,
                    b'"splitgraph_meta".'
                    b'"of0fb43e477311f82aa30055be303ff00599dfe155d737def0d00f06e07228b"',
                    ["fruit_id", "name"],
                    mock.ANY,
                    ("3",),
                ),
                # ...and one to query the applied fragments in the second group.
                mock.call(pg_repo_local.engine, mock.ANY, ["fruit_id", "name"], mock.ANY, ("3",),),
            ]

            # Check the temporary table has been deleted since we've exhausted the query
            args, _ = apply_fragments.call_args_list[0]
            tmp_table = args[2]
            assert not pg_repo_local.engine.table_exists(SPLITGRAPH_META_SCHEMA, tmp_table)

    # Now query PKs 3 and 4. Even though the chunk containing PKs 4 and 5 was updated
    # (by changing PK 5), the qual filter should drop the update, as it's not pertinent
    # to the query. Hence, we should end up not needing fragment application.
    with mock.patch.object(
        PostgresEngine, "apply_fragments", wraps=pg_repo_local.engine.apply_fragments
    ) as apply_fragments:
        with mock.patch(
            "splitgraph.core.table._generate_select_query", side_effect=_generate_select_query
        ) as _gsc:
            assert list(
                fruits.query(
                    columns=["fruit_id", "name"],
                    quals=[[("fruit_id", "=", "3"), ("fruit_id", "=", "4")]],
                )
            ) == [{"fruit_id": 3, "name": "mayonnaise"}, {"fruit_id": 4, "name": "fruit_4"}]

            # No fragment application
            assert apply_fragments.call_count == 0

            # Single call to _generate_select_queries directly selecting rows from the two chunks
            assert _gsc.mock_calls == [
                call(
                    mock.ANY,
                    b'"splitgraph_meta".'
                    b'"of0fb43e477311f82aa30055be303ff00599dfe155d737def0d00f06e07228b"',
                    ["fruit_id", "name"],
                    mock.ANY,
                    ("3", "4"),
                ),
                call(
                    mock.ANY,
                    b'"splitgraph_meta".'
                    b'"oaa6d009e485bfa91aec4ab6b0ed1ebcd67055f6a3420d29f26446b034f41cc"',
                    ["fruit_id", "name"],
                    mock.ANY,
                    ("3", "4"),
                ),
            ]


def test_disjoint_table_lq_two_singletons_one_overwritten_indirect(pg_repo_local):
    # Now test scanning the dataset with two singletons and one non-singleton group
    # by consuming queries one-by-one.

    prepare_lq_repo(pg_repo_local, commit_after_every=True, include_pk=True)
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (4, 'fruit_4'), (5, 'fruit_5')")
    fruits = pg_repo_local.commit().get_table("fruits")

    tables, callback, _ = fruits.query_indirect(columns=["fruit_id", "name"], quals=None)

    # At this point, we've "claimed" all objects but haven't done anything with them.
    # We're not really testing object claiming here since the objects were created locally
    # (see test_object_cache_deferred in test_object_cache.py for a test for claims/releases)
    assert len(callback) == 1

    # First, we emit cstore tables
    assert (
        next(tables) == b'"splitgraph_meta".'
        b'"of0fb43e477311f82aa30055be303ff00599dfe155d737def0d00f06e07228b"'
    )
    assert len(callback) == 1

    # There's another singleton we can query directly.
    assert (
        next(tables) == b'"splitgraph_meta".'
        b'"oaa6d009e485bfa91aec4ab6b0ed1ebcd67055f6a3420d29f26446b034f41cc"'
    )
    assert len(callback) == 1

    # We have two fragments left to scan but they overlap each other, so they have to be materialized.
    with mock.patch.object(
        PostgresEngine, "apply_fragments", wraps=pg_repo_local.engine.apply_fragments
    ) as apply_fragments:
        next(tables)
        assert apply_fragments.call_count == 1
        args, _ = apply_fragments.call_args_list[0]
        tmp_table = args[2]

    # Because of this, our callback list now includes deleting the temporary table
    assert len(callback) == 2

    # We've now exhausted the list of queries
    with pytest.raises(StopIteration):
        next(tables)

    # ...but haven't called the callback yet
    assert pg_repo_local.engine.table_exists(SPLITGRAPH_META_SCHEMA, tmp_table)

    # Call the callback now, deleting the temporary table.
    callback()
    assert not pg_repo_local.engine.table_exists(SPLITGRAPH_META_SCHEMA, tmp_table)


def test_disjoint_table_lq_temp_table_deletion_doesnt_lock_up(pg_repo_local):
    # When Multicorn reads from the temporary table, it does that in the context of the
    # transaction that it's been called from. It hence can hold a read lock on the
    # temporary table that it won't release until the scan is over but the scan won't
    # be over until we've managed to delete the temporary table, leading to a deadlock.
    # Check deleting the table in a separate thread works.

    prepare_lq_repo(pg_repo_local, commit_after_every=True, include_pk=True)
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (4, 'fruit_4'), (5, 'fruit_5')")
    fruits = pg_repo_local.commit().get_table("fruits")

    tables, callback, plan = fruits.query_indirect(columns=["fruit_id", "name"], quals=None)

    # Force a materialization and get the query that makes us read from the temporary table.
    last_table = list(tables)[-1]
    pg_repo_local.commit_engines()

    # Simulate Multicorn reading from the table by doing it from a different engine.
    conn_params = _prepare_engine_config(CONFIG)
    engine = PostgresEngine(conn_params=conn_params, name="test_engine")
    query = _generate_select_query(engine, last_table, ["fruit_id", "name"])
    engine.run_sql(query)
    logging.info("Acquired read lock")

    # At this point, we're holding a lock on the table and callback will lock up, unless it's run
    # in a separate thread.
    callback(from_fdw=True)

    # Wait for the thread to actually start (a bit gauche but otherwise makes the test flaky(ier))
    time.sleep(1)

    # We still can read from the table (callback spawned a thread which is locked trying to delete it).
    engine.run_sql(query)

    # Now drop the lock.
    logging.info("Dropping the lock")
    engine.rollback()

    # If this test assertion fails, make sure we're not left holding the lock.
    engine.autocommit = True
    with pytest.raises(ObjectNotFoundError):
        engine.run_sql(query)


def test_get_chunk_groups():
    # Two non-overlapping chunks
    assert get_chunk_groups([("chunk_1", 1, 2), ("chunk_2", 3, 4)]) == [
        [("chunk_1", 1, 2)],
        [("chunk_2", 3, 4)],
    ]

    # Two overlapping chunks
    assert get_chunk_groups([("chunk_1", 1, 3), ("chunk_2", 3, 4)]) == [
        [("chunk_1", 1, 3), ("chunk_2", 3, 4)]
    ]

    # Two non-overlapping chunks, wrong order (groups ordered by first key in chunk)
    assert get_chunk_groups([("chunk_1", 2, 4), ("chunk_2", 1, 1)]) == [
        [("chunk_2", 1, 1)],
        [("chunk_1", 2, 4)],
    ]

    # Three chunks, first and last overlap, original order preserved in-group
    assert get_chunk_groups([("one", 2, 4), ("two", 5, 6), ("three", 1, 2)]) == [
        [("one", 2, 4), ("three", 1, 2)],
        [("two", 5, 6)],
    ]

    # Four chunks, overlaps: 1-3, 2-4 -- should make two groups
    assert get_chunk_groups([("one", 1, 3), ("two", 6, 8), ("three", 3, 5), ("four", 8, 10)]) == [
        [("one", 1, 3), ("three", 3, 5)],
        [("two", 6, 8), ("four", 8, 10)],
    ]

    # Four chunks, overlaps: 1-3, 2-4, 3-2, makes one big group even though 1 doesn't directly overlap with 4
    assert get_chunk_groups([("one", 1, 3), ("two", 6, 8), ("three", 3, 6), ("four", 8, 10)]) == [
        [("one", 1, 3), ("two", 6, 8), ("three", 3, 6), ("four", 8, 10)]
    ]
