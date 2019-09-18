"""
Miscellaneous image management sgr commands.
"""
import sys

import click

from splitgraph import CONFIG
from splitgraph.config.export import serialize_config
from splitgraph.core.engine import init_engine, repository_exists
from splitgraph.core.object_manager import ObjectManager
from splitgraph.core.repository import Repository
from splitgraph.engine import get_engine
from splitgraph.exceptions import CheckoutError
from ._common import ImageType, RepositoryType


@click.command(name="rm")
@click.argument("image_spec", type=ImageType(default=None))
@click.option(
    "-r", "--remote", help="Perform the deletion on a remote instead, specified by its alias"
)
@click.option(
    "-y", "--yes", help="Agree to deletion without confirmation", is_flag=True, default=False
)
def rm_c(image_spec, remote, yes):
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

    ``sgr rm --remote splitgraph.com username/repo``

    Deletes ``username/repo`` from the Splitgraph registry.

    ``sgr rm -y username/repo:old_branch``

    Deletes the image pointed to by ``old_branch`` as well as all of its children (images created by a commit based
    on this image), as well as all of the tags that point to now deleted images, without asking for confirmation.
    Note this will not delete images that import tables from the deleted images via Splitfiles or indeed the
    physical objects containing the actual tables.
    """

    repository, image = image_spec
    repository = Repository.from_template(repository, engine=get_engine(remote or "LOCAL"))
    if not image:
        click.echo(
            ("Repository" if repository_exists(repository) else "Postgres schema")
            + " %s will be deleted." % repository.to_schema()
        )
        if not yes:
            click.confirm("Continue? ", abort=True)
        repository.delete()
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
@click.argument("repository", type=RepositoryType())
@click.option(
    "-r", "--remote", help="Perform the deletion on a remote instead, specified by its alias"
)
@click.option(
    "-y", "--yes", help="Agree to deletion without confirmation", is_flag=True, default=False
)
def prune_c(repository, remote, yes):
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
    repository = Repository.from_template(repository, engine=get_engine(remote or "LOCAL"))

    all_images = set(image.image_hash for image in repository.images())
    all_tagged_images = {i for i, t in repository.get_all_hashes_tags()}
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
    deleted = ObjectManager(get_engine()).cleanup()
    click.echo("Deleted %d physical object(s)" % len(deleted))


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
def config_c(no_shielding, config_format):
    """
    Print the current Splitgraph configuration.

    This takes into account the local config file, the default values
    and all overrides specified via environment variables.

    This command can be used to dump the current Splitgraph configuration into a file:

        sgr config --no-shielding --config-format > .sgconfig

    ...or save a config file overriding an entry:

        SG_REPO_LOOKUP=engine1,engine2 sgr config -sc > .sgconfig
    """

    click.echo(serialize_config(CONFIG, config_format, no_shielding))


@click.command(name="dump")
@click.argument("repository", type=RepositoryType())
@click.option("--exclude-object-contents", is_flag=True, default=False)
def dump_c(repository, exclude_object_contents):
    """
    Dump a repository to SQL.
    """
    repository.dump(sys.stdout, exclude_object_contents=exclude_object_contents)
