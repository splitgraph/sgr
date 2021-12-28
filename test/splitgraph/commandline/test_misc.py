import os
import tempfile
from pathlib import Path
from test.splitgraph.commands.test_commit_diff import _alter_diff_splitting_dataset
from test.splitgraph.conftest import API_RESOURCES, OUTPUT
from unittest import mock
from unittest.mock import call, patch, sentinel

import httpretty
import pytest
from click import ClickException
from click.testing import CliRunner

from splitgraph.commandline import (
    cli,
    config_c,
    dump_c,
    eval_c,
    import_c,
    prune_c,
    rm_c,
    upstream_c,
)
from splitgraph.commandline.common import ImageType, RepositoryType
from splitgraph.commandline.example import alter_c, generate_c, splitfile_c
from splitgraph.commandline.misc import (
    _get_binary_url_for,
    _get_download_paths,
    _get_system_id,
    upgrade_c,
)
from splitgraph.config import PG_PWD, PG_USER
from splitgraph.core.engine import repository_exists
from splitgraph.core.fragment_manager import FragmentManager
from splitgraph.core.repository import Repository
from splitgraph.engine import ResultShape
from splitgraph.exceptions import (
    ImageNotFoundError,
    RepositoryNotFoundError,
    TableNotFoundError,
)


def test_image_spec_parsing():
    assert ImageType()("test/pg_mount") == (Repository("test", "pg_mount"), "latest")
    assert ImageType(default="HEAD")("test/pg_mount") == (Repository("test", "pg_mount"), "HEAD")
    assert ImageType()("test/pg_mount:some_tag") == (Repository("test", "pg_mount"), "some_tag")
    assert ImageType()("pg_mount") == (Repository("", "pg_mount"), "latest")
    assert ImageType()("pg_mount:some_tag") == (Repository("", "pg_mount"), "some_tag")
    assert ImageType(default="HEAD")("pg_mount:some_tag") == (
        Repository("", "pg_mount"),
        "some_tag",
    )


def test_image_repo_parsing_errors(pg_repo_local):
    repo = Repository("test", "pg_mount")
    assert ImageType(get_image=True, default="latest")("test/pg_mount")[1] == repo.images["latest"]
    assert (
        ImageType(get_image=True, default="latest")("test/pg_mount:00000000")[1]
        == repo.images["00000000"]
    )

    with pytest.raises(ImageNotFoundError):
        ImageType(get_image=True, default="latest")("test/pg_mount:doesnt_exist")

    with pytest.raises(RepositoryNotFoundError):
        ImageType(get_image=True, default="latest")("test/doesntexist:latest")

    with pytest.raises(RepositoryNotFoundError):
        RepositoryType(exists=True)("test/doesntexist")


def test_upstream_management(pg_repo_local):
    runner = CliRunner()

    # sgr upstream test/pg_mount
    result = runner.invoke(upstream_c, ["test/pg_mount"])
    assert result.exit_code == 0
    assert "has no upstream" in result.output

    # Set to nonexistent engine
    result = runner.invoke(upstream_c, ["test/pg_mount", "--set", "dummy_engine", "test/pg_mount"])
    assert result.exit_code == 1
    assert "Remote engine 'dummy_engine' does not exist" in result.output

    # Set to existing engine (should we check the repo actually exists?)
    result = runner.invoke(upstream_c, ["test/pg_mount", "--set", "remote_engine", "test/pg_mount"])
    assert result.exit_code == 0
    assert "set to track remote_engine:test/pg_mount" in result.output

    # Get upstream again
    result = runner.invoke(upstream_c, ["test/pg_mount"])
    assert result.exit_code == 0
    assert "is tracking remote_engine:test/pg_mount" in result.output

    # Reset it
    result = runner.invoke(upstream_c, ["test/pg_mount", "--reset"])
    assert result.exit_code == 0
    assert "Deleted upstream for test/pg_mount" in result.output
    assert pg_repo_local.upstream is None

    # Reset it again
    result = runner.invoke(upstream_c, ["test/pg_mount", "--reset"])
    assert result.exit_code == 1
    assert "has no upstream" in result.output


@pytest.mark.mounting
def test_import(pg_repo_local, mg_repo_local):
    runner = CliRunner()
    head = pg_repo_local.head

    # sgr import mountpoint, table, target_mountpoint (3-arg)
    result = runner.invoke(import_c, [str(mg_repo_local), "stuff", str(pg_repo_local)])
    assert result.exit_code == 0
    new_head = pg_repo_local.head
    assert new_head.get_table("stuff")

    with pytest.raises(TableNotFoundError):
        head.get_table("stuff")

    # sgr import with alias
    result = runner.invoke(
        import_c, [str(mg_repo_local), "stuff", str(pg_repo_local), "stuff_copy"]
    )
    assert result.exit_code == 0
    new_new_head = pg_repo_local.head
    assert new_new_head.get_table("stuff_copy")

    with pytest.raises(TableNotFoundError):
        new_head.get_table("stuff_copy")

    # sgr import with alias and custom image hash
    mg_repo_local.run_sql("DELETE FROM stuff")
    new_mg_head = mg_repo_local.commit()

    result = runner.invoke(
        import_c,
        [
            str(mg_repo_local) + ":" + new_mg_head.image_hash,
            "stuff",
            str(pg_repo_local),
            "stuff_empty",
        ],
    )
    assert result.exit_code == 0
    new_new_new_head = pg_repo_local.head
    assert new_new_new_head.get_table("stuff_empty")

    with pytest.raises(TableNotFoundError):
        new_new_head.get_table("stuff_empty")

    assert pg_repo_local.run_sql("SELECT * FROM stuff_empty") == []

    # sgr import with query, no alias
    result = runner.invoke(
        import_c,
        [
            str(mg_repo_local) + ":" + new_mg_head.image_hash,
            "SELECT * FROM stuff",
            str(pg_repo_local),
        ],
    )
    assert result.exit_code != 0
    assert "TARGET_TABLE is required" in str(result.stdout)


def test_rm_repositories(pg_repo_local, pg_repo_remote):
    runner = CliRunner()

    # sgr rm test/pg_mount, say "no"
    result = runner.invoke(rm_c, [str(pg_repo_local)], input="n\n")
    assert result.exit_code == 1
    assert "Repository test/pg_mount will be deleted" in result.output
    assert repository_exists(pg_repo_local)

    # sgr rm test/pg_mount, say "yes"
    result = runner.invoke(rm_c, [str(pg_repo_local)], input="y\n")
    assert result.exit_code == 0
    assert not repository_exists(pg_repo_local)

    # sgr rm test/pg_mount -r remote_engine
    result = runner.invoke(rm_c, [str(pg_repo_remote), "-r", "remote_engine"], input="y\n")
    assert result.exit_code == 0
    assert not repository_exists(pg_repo_remote)


def test_rm_images(pg_repo_local_multitag, pg_repo_remote_multitag):
    # Play around with both engines for simplicity -- both have 2 images with 2 tags
    runner = CliRunner()
    local_v1 = pg_repo_local_multitag.images["v1"].image_hash
    local_v2 = pg_repo_local_multitag.images["v2"].image_hash

    # Test deleting checked out image causes an error
    result = runner.invoke(rm_c, [str(pg_repo_local_multitag) + ":v2"])
    assert result.exit_code != 0
    assert "do sgr checkout -u test/pg_mount" in str(result.exc_info)

    pg_repo_local_multitag.uncheckout()

    # sgr rm test/pg_mount:v2, say "no"
    result = runner.invoke(rm_c, [str(pg_repo_local_multitag) + ":v2"], input="n\n")
    assert result.exit_code == 1
    # Specify most of the output verbatim here to make sure it's not proposing
    # to delete more than needed (just the single image and the single v2 tag)
    assert (
        "Images to be deleted:\n" + local_v2 + "\nTotal: 1\n\nTags to be deleted:\nv2\nTotal: 1"
        in result.output
    )
    # Since we cancelled the operation, 'v2' still remains.
    assert pg_repo_local_multitag.images["v2"].image_hash == local_v2
    assert pg_repo_local_multitag.images[local_v2] is not None

    # Uncheckout the remote too (it's supposed to be bare anyway)
    remote_v2 = pg_repo_remote_multitag.images["v2"].image_hash
    pg_repo_remote_multitag.uncheckout()

    # sgr rm test/pg_mount:v2 -r remote_engine, say "yes"
    result = runner.invoke(
        rm_c, [str(pg_repo_remote_multitag) + ":v2", "-r", "remote_engine"], input="y\n"
    )
    assert result.exit_code == 0
    assert pg_repo_remote_multitag.images.by_tag("v2", raise_on_none=False) is None

    with pytest.raises(ImageNotFoundError):
        pg_repo_remote_multitag.images.by_hash(remote_v2)

    # sgr rm test/pg_mount:v1 -y
    # Should delete both images since v2 depends on v1
    result = runner.invoke(rm_c, [str(pg_repo_local_multitag) + ":v1", "-y"])
    assert result.exit_code == 0
    assert local_v2 in result.output
    assert local_v1 in result.output
    assert "v1" in result.output
    assert "v2" in result.output
    # One image remaining (the 00000.. base image)
    assert len(pg_repo_local_multitag.images()) == 1


def test_prune(pg_repo_local_multitag, pg_repo_remote_multitag):
    runner = CliRunner()
    # Two engines, two repos, two images in each (tagged v1 and v2, v1 is the parent of v2).
    pg_repo_remote_multitag.uncheckout()

    # sgr prune test/pg_mount -- all images are tagged, nothing to do.
    result = runner.invoke(prune_c, [str(pg_repo_local_multitag)])
    assert result.exit_code == 0
    assert "Nothing to do" in result.output

    # Delete tag v2 and run sgr prune -r remote_engine test/pg_mount, say "no": the image
    # that used to be 'v2' now isn't tagged so it will be a candidate for removal (but not the v1 image).
    remote_v2 = pg_repo_remote_multitag.images["v2"]
    remote_v2.delete_tag("v2")
    pg_repo_remote_multitag.commit_engines()

    result = runner.invoke(
        prune_c, [str(pg_repo_remote_multitag), "-r", "remote_engine"], input="n\n"
    )
    assert result.exit_code == 1  # Because "n" aborted the command
    assert remote_v2.image_hash in result.output
    assert "Total: 1" in result.output
    # Make sure the image still exists
    assert pg_repo_remote_multitag.images.by_hash(remote_v2.image_hash)

    # Delete tag v1 and run sgr prune -r remote_engine -y test_pg_mount:
    # now both images aren't tagged so will get removed.
    remote_v1 = pg_repo_remote_multitag.images["v1"]
    remote_v1.delete_tag("v1")
    pg_repo_remote_multitag.commit_engines()
    result = runner.invoke(prune_c, [str(pg_repo_remote_multitag), "-r", "remote_engine", "-y"])
    assert result.exit_code == 0
    assert remote_v2.image_hash in result.output
    assert remote_v1.image_hash in result.output
    # 2 images + the 000... image
    assert "Total: 3" in result.output
    assert not pg_repo_remote_multitag.images()

    # Finally, delete both tags from the local engine and prune. Since there's still
    # a HEAD tag pointing to the ex-v2, nothing will actually happen.
    result = runner.invoke(prune_c, [str(pg_repo_local_multitag), "-y"])
    assert "Nothing to do." in result.output
    # 2 images + the 000.. image
    assert len(pg_repo_local_multitag.images()) == 3
    assert len(pg_repo_local_multitag.get_all_hashes_tags()) == 3


def test_config_dumping():
    runner = CliRunner()

    # sgr config (normal, with passwords shielded)
    result = runner.invoke(config_c, catch_exceptions=False)
    assert result.exit_code == 0
    assert PG_PWD not in result.output
    assert "remote_engine:" in result.output
    assert ("SG_ENGINE_USER=%s" % PG_USER) in result.output
    assert "DUMMY=test.splitgraph.splitfile" in result.output
    assert "S3=splitgraph.hooks.s3" in result.output

    # sgr config -s (no password shielding)
    result = runner.invoke(config_c, ["-s"])
    assert result.exit_code == 0
    assert ("SG_ENGINE_USER=%s" % PG_USER) in result.output
    assert ("SG_ENGINE_PWD=%s" % PG_PWD) in result.output
    assert "remote_engine:" in result.output

    # sgr config -sc (no password shielding, output in config format)
    result = runner.invoke(config_c, ["-sc"])
    assert result.exit_code == 0
    assert ("SG_ENGINE_USER=%s" % PG_USER) in result.output
    assert ("SG_ENGINE_PWD=%s" % PG_PWD) in result.output
    assert "[remote: remote_engine]" in result.output
    assert "[defaults]" in result.output
    assert "[commands]" in result.output
    assert "[external_handlers]" in result.output
    assert "[data_sources]" in result.output
    assert "S3=splitgraph.hooks.s3" in result.output

    # sgr config -n (print connection string to engine)
    result = runner.invoke(config_c, ["-n"])
    assert result.output == "postgresql://sgr:supersecure@localhost:5432/splitgraph\n"


def test_examples(local_engine_empty):
    # Test the example-generating commands used in the quickstart

    runner = CliRunner()
    result = runner.invoke(generate_c, ["example/repo_1"])
    assert result.exit_code == 0

    repo = Repository.from_schema("example/repo_1")
    assert len(repo.images()) == 2
    assert repo.run_sql("SELECT COUNT(*) FROM demo", return_shape=ResultShape.ONE_ONE) == 10
    assert repo.diff("demo", repo.head, None, aggregate=True) == (0, 0, 0)

    result = runner.invoke(alter_c, ["example/repo_1"])
    assert result.exit_code == 0
    assert len(repo.images()) == 2
    assert repo.diff("demo", repo.head, None, aggregate=True) == (2, 2, 2)

    result = runner.invoke(splitfile_c, ["example/repo_1", "example/repo_2"])
    assert result.exit_code == 0
    assert "FROM example/repo_1 IMPORT demo AS table_1" in result.stdout
    assert "FROM example/repo_2:${IMAGE_2} IMPORT demo AS table_2" in result.stdout


def test_commandline_dump_load(pg_repo_local):
    pg_repo_local.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    pg_repo_local.commit()
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    pg_repo_local.commit()
    pg_repo_local.run_sql("UPDATE fruits SET name = 'banana' WHERE fruit_id = 1")
    pg_repo_local.commit()
    pg_repo_local.head.tag("test_tag")

    runner = CliRunner()
    result = runner.invoke(dump_c, [str(pg_repo_local)], catch_exceptions=False)
    assert result.exit_code == 0

    dump = result.stdout

    # Now delete the repo and try loading the dump to test it actually works.
    pg_repo_local.delete()
    pg_repo_local.objects.cleanup()

    pg_repo_local.engine.run_sql(dump)

    pg_repo_local.images["test_tag"].checkout()

    assert pg_repo_local.run_sql("SELECT * FROM fruits ORDER BY fruit_id") == [
        (1, "banana"),
        (2, "orange"),
        (3, "mayonnaise"),
    ]


def test_commandline_eval():
    runner = CliRunner()

    result = runner.invoke(eval_c, ["print()"], input="n\n", catch_exceptions=False)
    assert result.exit_code == 1
    assert "Aborted!" in result.output

    result = runner.invoke(
        eval_c,
        [
            "assert Repository.from_schema('test/repo').namespace == 'test';"
            "assert object_manager is not None; print('arg_1=%s' % arg_1)",
            "--arg",
            "arg_1",
            "val_1",
            "--i-know-what-im-doing",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "arg_1=val_1" in result.output


_GH_TAG = "https://api.github.com/repos/splitgraph/splitgraph/releases/tags/v0.1.0"
_GH_LATEST = "https://api.github.com/repos/splitgraph/splitgraph/releases/latest"
_GH_NONEXISTENT = "https://api.github.com/repos/splitgraph/splitgraph/releases/tags/vnonexistent"


def _gh_response(request, uri, response_headers):
    with open(os.path.join(API_RESOURCES, "github_releases.json")) as f:
        return [200, response_headers, f.read()]


def _gh_404(request, uri, response_headers):
    return [404, response_headers, ""]


@httpretty.activate(allow_net_connect=False)
@pytest.mark.parametrize(
    ("system", "release", "result"),
    [
        (
            "linux",
            "latest",
            (
                "0.1.0",
                "https://github.com/splitgraph/splitgraph/releases/download/v0.1.0/sgr-linux-x86_64",
            ),
        ),
        (
            "linux",
            "v0.1.0",
            (
                "0.1.0",
                "https://github.com/splitgraph/splitgraph/releases/download/v0.1.0/sgr-linux-x86_64",
            ),
        ),
        (
            "osx",
            "latest",
            (
                "0.1.0",
                "https://github.com/splitgraph/splitgraph/releases/download/v0.1.0/sgr-osx-x86_64",
            ),
        ),
        (
            "windows",
            "latest",
            (
                "0.1.0",
                "https://github.com/splitgraph/splitgraph/releases/download/v0.1.0/sgr-windows-x86_64.exe",
            ),
        ),
        ("windows", "vnonexistent", ValueError),
        ("weirdplatform", "v0.1.0", ValueError),
    ],
)
def test_get_binary_url(system, release, result):
    httpretty.register_uri(httpretty.HTTPretty.GET, _GH_TAG, body=_gh_response)
    httpretty.register_uri(httpretty.HTTPretty.GET, _GH_LATEST, body=_gh_response)
    httpretty.register_uri(httpretty.HTTPretty.GET, _GH_NONEXISTENT, body=_gh_404)

    if result == ValueError:
        with pytest.raises(result):
            _get_binary_url_for(system, release)
    else:
        assert _get_binary_url_for(system, release) == result


def test_system_id_not_exists():
    with mock.patch("splitgraph.commandline.misc.platform.system", return_value="TempleOS"):
        with pytest.raises(ClickException):
            _get_system_id()


@pytest.mark.parametrize(
    ("path", "final_path"),
    [
        ("/home/user/", "/home/user/sgr"),
        ("/home/user/sgr_dest", "/home/user/sgr_dest"),
        (None, "/usr/local/bin/sgr"),
    ],
)
def test_get_download_paths(fs_fast, path, final_path):
    Path("/home/user/").mkdir(parents=True)

    with mock.patch("splitgraph.commandline.misc.sys") as m_sys:
        m_sys.executable = "/usr/local/bin/sgr"
        temp_path_actual, final_path_actual = _get_download_paths(
            path, "https://some.url.com/assets/sgr"
        )
        assert str(final_path_actual) == final_path


@httpretty.activate(allow_net_connect=False)
def test_upgrade_end_to_end():
    _BODY = "new sgr client"
    httpretty.register_uri(httpretty.HTTPretty.GET, _GH_TAG, body=_gh_response)
    httpretty.register_uri(httpretty.HTTPretty.GET, _GH_LATEST, body=_gh_response)
    httpretty.register_uri(httpretty.HTTPretty.GET, _GH_NONEXISTENT, body=_gh_404)

    httpretty.register_uri(
        httpretty.HTTPretty.GET,
        "https://github.com/splitgraph/splitgraph/releases/download/v0.1.0/sgr-linux-x86_64",
        body=_BODY,
        adding_headers={"Content-Length": len(_BODY)},
    )

    runner = CliRunner()

    # Patch a lot of things
    with tempfile.TemporaryDirectory() as dir:
        with open(os.path.join(dir, "sgr"), "w") as f:
            f.write("old sgr client")

        _module = "splitgraph.commandline.misc"
        with mock.patch(_module + ".sys") as m_sys:
            m_sys.executable = os.path.join(dir, "sgr")
            m_sys.frozen = True
            with mock.patch(_module + ".platform.system", return_value="Linux"):
                with mock.patch(_module + ".subprocess.check_call") as subprocess:
                    with mock.patch(_module + ".list_engines", return_value=[sentinel.engine]):
                        with mock.patch("splitgraph.commandline.misc.atexit.register") as register:
                            result = runner.invoke(upgrade_c, ["--force"], catch_exceptions=False)
                            assert result.exit_code == 0
                            print(result.output)

        assert subprocess.mock_calls == [
            call([mock.ANY, "--version"]),
            call([mock.ANY, "engine", "upgrade"]),
        ]

        # Call the atexit callback that swaps the new sgr in and check it does that correctly.
        # mock_calls is a list of tuples (name, args, kwargs), so grab the first arg
        finalize_callback = register.mock_calls[-1][1][0]
        assert finalize_callback.__name__ == "_finalize"
        finalize_callback()

        with open(os.path.join(dir, "sgr")) as f:
            assert f.read() == "new sgr client"
        with open(os.path.join(dir, "sgr.old")) as f:
            assert f.read() == "old sgr client"


def test_rollback_on_error(local_engine_empty):
    # For e.g. commit/checkout/other commands, we don't do commits/rollbacks
    # in the library itself and expect the caller to manage transactions. In CLI,
    # we need to make sure that erroneous transactions (e.g. interrupted SG commits)
    # are rolled back correctly instead of being committed.
    runner = CliRunner()

    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 VARCHAR, value_2 INTEGER)")
    for i in range(11):
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord("a") + i), i * 2))
    OUTPUT.commit(chunk_size=5, in_fragment_order={"test": ["key", "value_1"]})
    assert len(OUTPUT.images()) == 2
    assert len(OUTPUT.objects.get_all_objects()) == 3

    _alter_diff_splitting_dataset()
    OUTPUT.commit_engines()

    # Simulate the commit getting interrupted by the first object going through and being
    # recorded, then a KeyboardInterrupt being raised.
    called_once = False

    def interrupted_register(*args, **kwargs):
        nonlocal called_once
        if called_once:
            raise BaseException("something went wrong")
        else:
            called_once = True
            return FragmentManager._register_object(*args, **kwargs)

    with patch(
        "splitgraph.core.fragment_manager.FragmentManager._register_object",
        side_effect=interrupted_register,
    ) as ro:
        with pytest.raises(BaseException):
            runner.invoke(cli, ["commit", OUTPUT.to_schema()])

    # Check that no image/object metadata was written
    assert len(OUTPUT.images()) == 2
    assert len(OUTPUT.objects.get_all_objects()) == 3

    assert ro.call_count == 2

    # Check that the data in the audit trigger wasn't deleted
    assert len(OUTPUT.engine.get_pending_changes(OUTPUT.to_schema(), table="test")) == 6
