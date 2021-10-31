"""
Miscellaneous image management sgr commands.
"""
import atexit
import contextlib
import os
import platform
import shutil
import stat
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Optional, Tuple

import click
from packaging.version import Version
from splitgraph.__version__ import __version__
from splitgraph.exceptions import CheckoutError

from .common import ImageType, RepositoryType, remote_switch_option
from .engine import list_engines


@click.command(name="rm")
@click.argument("image_spec", type=ImageType(default=None))
@remote_switch_option()
@click.option(
    "-y", "--yes", help="Agree to deletion without confirmation", is_flag=True, default=False
)
def rm_c(image_spec, yes):
    """
    Delete schemas, repositories or images.

    If the target of this command is a Postgres schema, this performs DROP SCHEMA CASCADE.

    If the target of this command is a Splitgraph repository, this deletes the repository and all of its history.

    If the target of this command is an image, this deletes the image and all of its children.

    In any case, this command will ask for confirmation of the deletion, unless ``-y`` is passed. If ``-r``
    (``--remote``), is passed, this will perform deletion on a remote Splitgraph engine (registered in the config)
    instead, assuming the user has write access to the remote repository.

    This does not delete any physical objects that the deleted repository/images depend on:
    use ``sgr cleanup`` to do that.

    Examples:

    ``sgr rm temporary_schema``

    Deletes ``temporary_schema`` from the local engine.

    ``sgr rm --remote data.splitgraph.com username/repo``

    Deletes ``username/repo`` from the Splitgraph registry.

    ``sgr rm -y username/repo:old_branch``

    Deletes the image pointed to by ``old_branch`` as well as all of its children (images created by a commit based
    on this image), as well as all of the tags that point to now deleted images, without asking for confirmation.
    Note this will not delete images that import tables from the deleted images via Splitfiles or indeed the
    physical objects containing the actual tables.
    """
    from splitgraph.core.engine import repository_exists
    from splitgraph.core.repository import Repository
    from splitgraph.engine import get_engine

    engine = get_engine()

    repository, image = image_spec
    repository = Repository.from_template(repository, engine=engine)
    if not image:
        click.echo(
            ("Repository" if repository_exists(repository) else "Postgres schema")
            + " %s will be deleted." % repository.to_schema()
        )
        if not yes:
            click.confirm("Continue? ", abort=True)

        # Don't try to "uncheckout" repositories on the registry/other remote engines
        repository.delete(uncheckout=engine.name == "LOCAL")
        repository.commit_engines()
    else:
        image = repository.images[image]
        images_to_delete = repository.images.get_all_child_images(image.image_hash)
        tags_to_delete = [t for i, t in repository.get_all_hashes_tags() if i in images_to_delete]

        click.echo("Images to be deleted:")
        click.echo("\n".join(sorted(images_to_delete)))
        click.echo("Total: %d" % len(images_to_delete))

        click.echo("\nTags to be deleted:")
        click.echo("\n".join(sorted(tags_to_delete)))
        click.echo("Total: %d" % len(tags_to_delete))

        if "HEAD" in tags_to_delete:
            # If we're deleting an image that we currently have checked out,
            # we need to make sure the rest of the metadata (e.g. current state of the audit table) is consistent,
            # it's better to disallow these deletions completely.
            raise CheckoutError(
                "Deletion will affect a checked-out image! Check out a different branch "
                "or do sgr checkout -u %s!" % repository.to_schema()
            )
        if not yes:
            click.confirm("Continue? ", abort=True)

        repository.images.delete(images_to_delete)
        repository.commit_engines()
        click.echo("Success.")


@click.command(name="prune")
@click.argument("repository", type=RepositoryType(exists=True))
@remote_switch_option()
@click.option(
    "-y", "--yes", help="Agree to deletion without confirmation", is_flag=True, default=False
)
def prune_c(repository, yes):
    """
    Cleanup dangling images from a repository.

    This includes images not pointed to by any tags (or checked out) and those that aren't required by any of
    such images.

    Will ask for confirmation of the deletion, unless ``-y ``is passed. If ``-r`` (``--remote``) is
    passed, this will perform deletion on a remote Splitgraph engine (registered in the config) instead, assuming
    the user has write access to the remote repository.

    This does not delete any physical objects that the deleted repository/images depend on:
    use ``sgr cleanup`` to do that.
    """
    from splitgraph.core.repository import Repository
    from splitgraph.engine import get_engine

    repository = Repository.from_template(repository, engine=get_engine())

    all_images = {image.image_hash for image in repository.images()}
    all_tagged_images = {i for i, t in repository.get_all_hashes_tags() if i}
    dangling_images = all_images.difference(
        repository.images.get_all_parent_images(all_tagged_images)
    )

    if not dangling_images:
        click.echo("Nothing to do.")
        return

    click.echo("Images to be deleted:")
    click.echo("\n".join(sorted(dangling_images)))
    click.echo("Total: %d" % len(dangling_images))

    if not yes:
        click.confirm("Continue? ", abort=True)

    repository.images.delete(dangling_images)
    repository.commit_engines()
    click.echo("Success.")


@click.command(name="init")
@click.argument("repository", type=RepositoryType(), required=False, default=None)
@click.option("--skip-object-handling", default=False, is_flag=True)
def init_c(repository, skip_object_handling):
    """
    Initialize a new repository/engine.

    Examples:

    `sgr init`

    Initializes the current local Splitgraph engine by writing some bookkeeping information.
    This is required for the rest of sgr to work.

    `sgr init --skip-object-handling`

    Initializes a Splitgraph engine without installing audit triggers or object management routines:
    this is useful for engines that aren't intended to be used for image checkouts.

    ``sgr init new/repo``

    Creates a single image with the hash ``00000...`` in ``new/repo``
    """
    from splitgraph.core.engine import init_engine

    if repository:
        if skip_object_handling:
            raise click.BadOptionUsage(
                "--skip-object-handling", "Unsupported when initializing a new repository!"
            )
        repository.init()
        click.echo("Initialized empty repository %s" % str(repository))
    else:
        init_engine(skip_object_handling=skip_object_handling)


@click.command(name="cleanup")
def cleanup_c():
    """
    Prune unneeded objects from the engine.

    This deletes all objects from the cache that aren't required by any local repository.
    """
    from splitgraph.core.object_manager import ObjectManager
    from splitgraph.engine import get_engine

    from ..core.output import pluralise

    deleted = ObjectManager(get_engine()).cleanup()
    click.echo("Deleted %s." % pluralise("object", len(deleted)))


@click.command(name="config")
@click.option(
    "-s",
    "--no-shielding",
    is_flag=True,
    default=False,
    help="If set, doesn't replace sensitive values (like passwords) with asterisks",
)
@click.option(
    "-c",
    "--config-format",
    is_flag=True,
    default=False,
    help="Output configuration in the Splitgraph config file format",
)
@click.option(
    "-n",
    "--conn-string",
    is_flag=True,
    default=False,
    help="Print a libpq connection string to the engine",
)
def config_c(no_shielding, config_format, conn_string):
    """
    Print the current Splitgraph configuration.

    This takes into account the local config file, the default values
    and all overrides specified via environment variables.

    This command can be used to dump the current Splitgraph configuration into a file:

    ```
    sgr config --no-shielding --config-format > .sgconfig
    ```

    ...or save a config file overriding an entry:

    ```
    SG_REPO_LOOKUP=engine1,engine2 sgr config -sc > .sgconfig
    ```

    If `--conn-string` is passed, this prints out a libpq connection string
    that can be used to connect to the default Splitgraph engine with other tools:

    ```
    pgcli $(sgr config -n)
    ```
    """

    from splitgraph.config import CONFIG
    from splitgraph.config.export import serialize_config
    from splitgraph.engine import get_engine
    from splitgraph.engine.postgres.engine import get_conn_str

    if conn_string:
        click.echo(get_conn_str(get_engine().conn_params))
        return

    click.echo(serialize_config(CONFIG, config_format, no_shielding))


@click.command(name="dump")
@click.argument("repository", type=RepositoryType(exists=True))
@click.option(
    "--exclude-object-contents",
    is_flag=True,
    default=False,
    help="Don't dump the commands needed to recreate objects required by the repository.",
)
def dump_c(repository, exclude_object_contents):
    """
    Dump a repository to SQL.
    """
    repository.dump(sys.stdout, exclude_object_contents=exclude_object_contents)


def _eval(command, args):
    # appease PyCharm
    # noinspection PyUnresolvedReferences
    from splitgraph.core.object_manager import ObjectManager
    from splitgraph.core.repository import Repository  # noqa
    from splitgraph.engine import get_engine

    engine = get_engine()
    object_manager = ObjectManager(object_engine=engine, metadata_engine=engine)

    command_locals = locals().copy()
    command_locals.update({k: v for k, v in args})

    # The whole point of this function is to unsafely run Python code from the cmdline,
    # so silence the Bandit warning.
    exec(command, globals(), command_locals)  # nosec


@click.command(name="eval")
@click.option(
    "--i-know-what-im-doing",
    is_flag=True,
    help="Pass this if you're sure that the code you're running "
    "is safe and don't want to be prompted.",
)
@click.argument("command", type=str)
@click.option(
    "-a",
    "--arg",
    multiple=True,
    type=(str, str),
    help="Make extra variables available in the command's namespace",
)
def eval_c(i_know_what_im_doing, command, arg):
    """
    Evaluate a Python snippet using the Splitgraph API.

    This is for advanced users only and should be only used
    if you know what you are doing.

    Normal Python statements are supported and the command is evaluated
    in a namespace where the following is already imported and available:

      * Repository: class that instantiates a Splitgraph repository and makes
        API functions like .commit(), .checkout() etc available.

      * engine: Current local engine

      * object_manager: an instance of ObjectManager that allows
        to get information about objects and manage the object cache.

    \b
    Example:
    ```
    sgr eval 'import json; print(json.dumps(Repository\\
        .from_schema(repo_name)\\
        .images["latest"]\\
        .get_table(table_name)\\
        .table_schema))' \\
    -a repo_name my_repo -a table_name my_table
    ```

    Will dump the schema of table my_table in the most recent image in my_repo in JSON format.

    For more information, see the Splitgraph API reference.
    """
    if not i_know_what_im_doing:
        click.confirm(
            "WARNING: This command might be unsafe and break your Splitgraph \n"
            "installation or harm your machine. This is exactly the same as running \n"
            "untrusted code off of the Internet. In addition, it might rely on undocumented \n"
            "Splitgraph internals that might change in the future. \n\n"
            "Have you checked it and made sure it's not doing anything suspicious?",
            abort=True,
        )

    _eval(command, arg)


def _get_binary_url_for(system, release: str = "latest") -> Tuple[str, str]:
    import requests
    from splitgraph.cloud import get_headers

    if release == "latest":
        endpoint = "https://api.github.com/repos/splitgraph/splitgraph/releases/%s" % release
    else:
        endpoint = "https://api.github.com/repos/splitgraph/splitgraph/releases/tags/%s" % release
    headers = get_headers()
    headers.update({"Accept": "application/vnd.github.v3+json"})
    response = requests.get(endpoint, headers=get_headers())
    if response.status_code == 404:
        raise ValueError("No releases found for tag %s, system %s!" % (release, system))
    response.raise_for_status()

    body = response.json()
    actual_version = body["tag_name"].lstrip("v")
    executable = "sgr-%s-x86_64" % system
    if system == "windows":
        executable += ".exe"
    asset = [a for a in body["assets"] if a["name"] == executable]
    if not asset:
        raise ValueError("No releases found for tag %s, system %s!" % (release, system))
    return actual_version, asset[0]["browser_download_url"]


@click.command(name="upgrade")
@click.option("--skip-engine-upgrade", is_flag=True, help="Only upgrade the client")
@click.option("--path", help="Override the path to download the new binary to.")
@click.option("--force", is_flag=True, help="Reinstall older/same versions.")
@click.argument("version", default="latest")
def upgrade_c(skip_engine_upgrade, path, force, version):
    """Upgrade sgr client and engine.

    This will try to download the most recent stable binary for the current platform
    into the location this binary is running from and then upgrade the default engine.

    This method is only supported for single-binary installs and engines managed
    by `sgr engine`.
    """
    import requests
    from splitgraph.cloud import get_headers
    from splitgraph.config import CONFIG, SG_CMD_ASCII
    from tqdm import tqdm

    # Detect if we're running from a Pyinstaller binary
    if not hasattr(sys, "frozen") and not force and not path:
        raise click.ClickException(
            "Not running from a single binary. Use the tool "
            "you originally used to install Splitgraph (e.g. pip) "
            "to upgrade. To download and run a single binary "
            "for your platform, pass --force and --path."
        )

    system = _get_system_id()

    # Get link to the release
    actual_version, download_url = _get_binary_url_for(
        system, "v" + version if version != "latest" else version
    )

    if version == "latest":
        click.echo("Latest version is %s." % actual_version)

    if Version(actual_version) < Version(__version__) and not force:
        click.echo(
            "Newer or existing version of sgr (%s) is installed, pass --force to reinstall."
            % __version__
        )
        return

    # Download the file

    temp_path, final_path = _get_download_paths(path, download_url)

    # Delete the temporary file at exit if we crash

    def _unlink():
        with contextlib.suppress(FileNotFoundError):
            temp_path.unlink()

    atexit.register(_unlink)

    click.echo("Downloading sgr %s from %s to %s" % (actual_version, download_url, temp_path))

    headers = get_headers()
    response = requests.get(download_url, headers=headers, allow_redirects=True, stream=True)
    response.raise_for_status()

    with tqdm(
        total=int(response.headers["Content-Length"]), unit="B", unit_scale=True, ascii=SG_CMD_ASCII
    ) as pbar:
        with (open(str(temp_path), "wb")) as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
                pbar.update(len(chunk))

    # Test the new binary
    st = os.stat(str(temp_path))
    os.chmod(str(temp_path), st.st_mode | stat.S_IEXEC)
    subprocess.check_call([str(temp_path), "--version"])

    # Upgrade the default engine
    if not skip_engine_upgrade:
        containers = list_engines(include_all=True, prefix=CONFIG["SG_ENGINE_PREFIX"])
        if containers:
            click.echo("Upgrading the default engine...")
            subprocess.check_call([str(temp_path), "engine", "upgrade"])

    def _finalize():
        # On Windows we can't replace a running executable + probably should keep a backup anyway.
        # We hence rename ourselves to a .old file and ask the user to delete it if needed.
        if final_path.exists():
            backup_path = final_path.with_suffix(final_path.suffix + ".old")
            with contextlib.suppress(FileNotFoundError):
                # shutil.move() breaks on Windows otherwise.
                backup_path.unlink()
            shutil.move(str(final_path), str(backup_path))
            click.echo("Old version has been backed up to %s." % backup_path)

        shutil.move(str(temp_path), str(final_path))
        click.echo("New sgr has been installed at %s." % final_path)

    # Instead of moving the file right now, do it at exit -- that way
    # Pyinstaller won't break with a scary error when cleaning up because
    # the file it's running from has changed.
    atexit.register(_finalize)


def _get_download_paths(path: Optional[str], download_url: str) -> Tuple[Path, Path]:
    if path:
        path = os.path.abspath(path)
        if os.path.isdir(path):
            destdir = path
            file_name = download_url.split("/")[-1]
        else:
            destdir = os.path.dirname(path)
            file_name = os.path.basename(path)
    else:
        destdir = os.path.dirname(sys.executable)
        file_name = os.path.basename(sys.executable)
    final_path = Path(destdir) / file_name
    temp_path = Path(destdir) / uuid.uuid4().hex
    return temp_path, final_path


def _get_system_id():
    system_reported = platform.system()
    try:
        return {"Linux": "linux", "Windows": "windows", "Darwin": "osx"}[system_reported]
    except KeyError:
        raise click.ClickException("Single binary is unsupported on system %s!" % system_reported)
