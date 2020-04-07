import datetime

import pytest
from test.splitgraph.conftest import OUTPUT, load_splitfile, prepare_lq_repo

from splitgraph.core.repository import Repository
from splitgraph.splitfile import execute_commands
from splitgraph.splitfile.execution import rebuild_image


def test_provenance(local_engine_empty, pg_repo_remote_multitag):
    execute_commands(
        load_splitfile("import_remote_multiple.splitfile"), params={"TAG": "v1"}, output=OUTPUT
    )
    dependencies = OUTPUT.head.provenance()

    assert dependencies == [
        (pg_repo_remote_multitag, pg_repo_remote_multitag.images["v1"].image_hash)
    ]

    # Check reverse provenance. Since the repository lives on the remote engine, we need to
    # search for dependents on the local engine instead.
    source = pg_repo_remote_multitag.images["v1"]
    assert source.provenance(reverse=True, engine=local_engine_empty) == [
        (OUTPUT, OUTPUT.head.image_hash)
    ]

    assert source.provenance() == []


def test_provenance_with_from(local_engine_empty, pg_repo_remote_multitag):
    execute_commands(load_splitfile("from_remote.splitfile"), params={"TAG": "v1"}, output=OUTPUT)
    dependencies = OUTPUT.head.provenance()

    assert dependencies == [
        (pg_repo_remote_multitag, pg_repo_remote_multitag.images["v1"].image_hash)
    ]

    source = pg_repo_remote_multitag.images["v1"]
    assert source.provenance(reverse=True, engine=local_engine_empty) == [
        (OUTPUT, OUTPUT.head.image_hash)
    ]

    assert source.provenance() == []


def test_splitfile_recreate(local_engine_empty, pg_repo_remote_multitag):
    execute_commands(
        load_splitfile("import_with_custom_query_and_sql.splitfile"),
        params={"TAG": "v1"},
        output=OUTPUT,
    )
    recreated_commands = OUTPUT.head.to_splitfile()
    assert recreated_commands == [
        "FROM test/pg_mount:%s IMPORT {SELECT *\n" % pg_repo_remote_multitag.images["v1"].image_hash
        + """FROM fruits
WHERE name = 'orange'} AS my_fruits, {SELECT *
FROM vegetables
WHERE name LIKE '%o'} AS o_vegetables, vegetables AS vegetables, fruits AS all_fruits""",
        """SQL {CREATE TABLE test_table
  AS SELECT *
     FROM all_fruits}""",
    ]


def test_splitfile_recreate_custom_from(local_engine_empty, pg_repo_remote_multitag):
    execute_commands(load_splitfile("from_remote.splitfile"), params={"TAG": "v1"}, output=OUTPUT)
    recreated_commands = OUTPUT.head.to_splitfile()

    assert recreated_commands == [
        "FROM test/pg_mount:%s" % pg_repo_remote_multitag.images["v1"].image_hash,
        # Test provenance is recorded using the reformatted SQL
        """SQL {CREATE TABLE join_table
  AS SELECT fruit_id AS id
          , fruits.name AS fruit
          , vegetables.name AS vegetable
     FROM fruits
          INNER JOIN vegetables ON fruit_id = vegetable_id}""",
    ]


@pytest.mark.mounting
def test_splitfile_incomplete_provenance(local_engine_empty, pg_repo_remote_multitag):
    # This splitfile has a MOUNT as its first command. Check we emit partial splitfiles
    # instead of errors (we can't reproduce MOUNT commands).
    execute_commands(
        load_splitfile("import_from_mounted_db_with_sql.splitfile"),
        params={"TAG": "v1"},
        output=OUTPUT,
    )
    head_img = OUTPUT.head
    image_with_mount = head_img.get_log()[-2]
    recreated_commands = head_img.to_splitfile(ignore_irreproducible=True)

    assert recreated_commands == [
        "# Irreproducible Splitfile command of type MOUNT",
        "SQL {CREATE TABLE new_table\n" "  AS SELECT *\n" "     FROM all_fruits}",
    ]


def test_rerun_with_new_version(local_engine_empty, pg_repo_remote_multitag):
    # Test running commands that base new datasets on a remote repository.
    execute_commands(load_splitfile("from_remote.splitfile"), params={"TAG": "v1"}, output=OUTPUT)

    output_v1 = OUTPUT.head
    # Do a logical rebase of the newly created image on the V2 of the remote repository

    rebuild_image(output_v1, {pg_repo_remote_multitag: "v2"})
    output_v2 = OUTPUT.head
    assert local_engine_empty.run_sql("SELECT * FROM output.join_table") == [
        (2, "orange", "carrot")
    ]

    # Do some checks on the structure of the final output repo. In particular, make sure that the two derived versions
    # still exist and depend only on the respective tags of the source.
    v1 = pg_repo_remote_multitag.images["v1"]
    v2 = pg_repo_remote_multitag.images["v2"]
    assert output_v1.provenance() == [(pg_repo_remote_multitag, v1.image_hash)]
    assert output_v2.provenance() == [(pg_repo_remote_multitag, v2.image_hash)]

    ov1_log = output_v1.get_log()
    ov2_log = output_v2.get_log()

    # ov1_log: CREATE TABLE commit, then FROM v1
    # ov2_log: CREATE TABLE commit, then FROM v2 which is based on FROM v1 (since we cloned both from test/pg_mount),
    # then as previously.
    assert ov1_log[1:] == ov2_log[2:]


def test_rerun_with_from_import(local_engine_empty, pg_repo_remote_multitag):
    execute_commands(
        load_splitfile("import_remote_multiple.splitfile"), params={"TAG": "v1"}, output=OUTPUT
    )

    output_v1 = OUTPUT.head
    # Do a logical rebase of the newly created image on the V2 of the remote repository

    rebuild_image(output_v1, {pg_repo_remote_multitag: "v2"})
    output_v2 = OUTPUT.head

    # Do some checks on the structure of the final output repo. In particular, make sure that the two derived versions
    # still exist and depend only on the respective tags of the source.
    v1 = pg_repo_remote_multitag.images["v1"]
    v2 = pg_repo_remote_multitag.images["v2"]
    assert output_v1.provenance() == [(pg_repo_remote_multitag, v1.image_hash)]
    assert output_v2.provenance() == [(pg_repo_remote_multitag, v2.image_hash)]

    ov1_log = output_v1.get_log()
    ov2_log = output_v2.get_log()

    # ov1_log: CREATE TABLE commit, then IMPORT from v1, then the 00000 commit
    # ov2_log: CREATE TABLE commit, then FROM v1, then the 00000.. commit
    assert ov1_log[2:] == ov2_log[2:]
    assert len(ov1_log) == 3


def test_rerun_multiline_sql_roundtripping(pg_repo_local):
    # Test that with a multiline SQL sgr rebuild doesn't create a new image
    # when rebuilding the same one.
    execute_commands(load_splitfile("multiline_sql.splitfile"), output=OUTPUT)

    head = OUTPUT.head
    expected_sql = "SQL {INSERT INTO fruits \n" "VALUES (3, 'banana')\n" "     , (4, 'pineapple')}"

    assert head.to_splitfile()[1] == expected_sql

    rebuild_image(head, {})
    head_v2 = OUTPUT.head
    assert head_v2.to_splitfile()[1] == expected_sql
    assert head_v2 == head


def test_provenance_inline_sql(readonly_pg_repo, pg_repo_local):
    prepare_lq_repo(pg_repo_local, commit_after_every=False, include_pk=True)
    pg_repo_local.head.tag("v2")

    execute_commands(
        load_splitfile("inline_sql.splitfile"), output=OUTPUT,
    )

    new_head = OUTPUT.head

    remote_input = readonly_pg_repo.images["latest"]
    local_input = pg_repo_local.images["latest"]

    assert set(new_head.provenance()) == {
        (readonly_pg_repo, remote_input.image_hash,),
        (pg_repo_local, local_input.image_hash),
    }

    assert remote_input.provenance(reverse=True, engine=OUTPUT.engine) == [
        (OUTPUT, OUTPUT.head.image_hash)
    ]

    assert local_input.provenance(reverse=True, engine=OUTPUT.engine) == [
        (OUTPUT, OUTPUT.head.image_hash)
    ]
    expected_sql = (
        "SQL {{CREATE TABLE balanced_diet\n"
        "  AS SELECT fruits.fruit_id AS id\n"
        "          , fruits.name AS fruit\n"
        "          , my_fruits.timestamp AS timestamp\n"
        "          , vegetables.name AS vegetable\n"
        "     FROM "
        '"otheruser/pg_mount:{0}".fruits AS '
        "fruits\n"
        "          INNER JOIN "
        '"otheruser/pg_mount:{0}".vegetables '
        "AS vegetables ON fruits.fruit_id = vegetable_id\n"
        "          LEFT JOIN "
        '"test/pg_mount:{1}".fruits AS '
        "my_fruits ON my_fruits.fruit_id = fruits.fruit_id;\n"
        "\n"
        "ALTER TABLE balanced_diet ADD PRIMARY KEY (id)}}"
    ).format(remote_input.image_hash, local_input.image_hash)

    assert new_head.to_splitfile() == [expected_sql]

    assert new_head.to_splitfile(
        source_replacement={pg_repo_local: "new_local_tag", readonly_pg_repo: "new_remote_tag"}
    ) == [
        expected_sql.replace(remote_input.image_hash, "new_remote_tag").replace(
            local_input.image_hash, "new_local_tag"
        )
    ]

    assert len(OUTPUT.images()) == 2

    # Try rerunning the Splitfile against the same original data (check caching)
    rebuild_image(
        OUTPUT.head, source_replacement={pg_repo_local: "latest", readonly_pg_repo: "latest"},
    )

    assert len(OUTPUT.images()) == 2

    # Change pg_repo_local and rerun the Splitfile against it.
    pg_repo_local.run_sql("UPDATE fruits SET timestamp = '2020-01-01 12:00:00' WHERE fruit_id = 2")
    new_head = pg_repo_local.commit()

    rebuild_image(
        OUTPUT.head,
        source_replacement={pg_repo_local: new_head.image_hash, readonly_pg_repo: "latest"},
    )

    assert len(OUTPUT.images()) == 3
    assert OUTPUT.run_sql("SELECT * FROM balanced_diet") == [
        (1, "apple", None, "potato"),
        (2, "orange", datetime.datetime(2020, 1, 1, 12, 0), "carrot"),
    ]
