import pytest

from splitgraph.core import clone


def _prepare_lq_repo(repo, include_snap, commit_after_every, include_pk):
    OPS = ["INSERT INTO fruits VALUES (3, 'mayonnaise')",
           "DELETE FROM fruits WHERE name = 'apple'",
           "DELETE FROM vegetables WHERE vegetable_id = 1",
           "UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2"]

    repo.run_sql("ALTER TABLE fruits ADD COLUMN number NUMERIC DEFAULT 1")
    if include_pk:
        repo.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
        repo.run_sql("ALTER TABLE vegetables ADD PRIMARY KEY (vegetable_id)")

    for o in OPS:
        repo.run_sql(o)
        print(o)

        if commit_after_every:
            repo.commit(include_snap=include_snap)
    if not commit_after_every:
        repo.commit(include_snap=include_snap)


@pytest.mark.parametrize("include_snap", [True, False])
@pytest.mark.parametrize("commit_after_every", [True, False])
@pytest.mark.parametrize("include_pk", [True, False])
def test_layered_querying(pg_repo_local, include_snap, commit_after_every, include_pk):
    _prepare_lq_repo(pg_repo_local, include_snap, commit_after_every, include_pk)
    new_head = pg_repo_local.head
    # Discard the actual materialized table and query everything via FDW
    new_head.checkout(layered=True)

    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id = 3") == [(3, 'mayonnaise', 1)]
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id = 2") == [(2, 'guitar', 1)]
    assert pg_repo_local.run_sql("SELECT * FROM vegetables WHERE vegetable_id = 1") == []
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id = 1") == []

    # Test some more esoteric qualifiers
    # EQ on string
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE name = 'guitar'") == [(2, 'guitar', 1)]

    # IN ( converted to =ANY(array([...]))
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE name IN ('guitar', 'mayonnaise') ORDER BY fruit_id") == \
           [(2, 'guitar', 1), (3, 'mayonnaise', 1)]

    # LIKE (operator ~~)
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE name LIKE '%uitar'") == [(2, 'guitar', 1)]

    # Join between two FDWs
    assert pg_repo_local.run_sql("SELECT * FROM fruits JOIN vegetables ON fruits.fruit_id = vegetables.vegetable_id")\
           == [(2, 'guitar', 1, 2, 'carrot')]

    # Expression in terms of another column
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id = number") == []
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id = number + 1 ") == [(2, 'guitar', 1)]


@pytest.mark.xfail(reason="FDW on the engine can't yet download objects from the remote on demand")
def test_lq_remote(local_engine_empty, pg_repo_remote):
    # Test layered querying works when we initialize it on a cloned repo that doesn't have any
    # external objects.

    # 1 DIFF on top of fruits, 1 DIFF on top of vegetables
    _prepare_lq_repo(pg_repo_remote, include_snap=False, commit_after_every=False, include_pk=True)
    pg_repo_local = clone(pg_repo_remote, download_all=False)
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 0
    assert len(pg_repo_remote.objects.get_downloaded_objects()) == 4  # 2 SNAPs, 2 DIFFs

    # Do a lazy LQ checkout -- no objects should be downloaded yet
    pg_repo_local.images['latest'].checkout(layered=True)
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 0

    # Actual LQ still downloads the objects, but one by one.

    # Hit fruits -- 2 objects should be downloaded
    try:
        assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id = 2") == [(2, 'guitar', 1)]
    except Exception:
        import pdb;
        pdb.set_trace()
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 2

    # Hit vegetables -- 2 more objects should be downloaded
    assert pg_repo_local.run_sql("SELECT * FROM vegetables WHERE vegetable_id = 1") == []
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 4
