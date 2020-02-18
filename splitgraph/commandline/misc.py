"""
Miscellaneous image management sgr commands.
"""
import sys

import click

from splitgraph.exceptions import CheckoutError
from .common import ImageType, RepositoryType


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
    from splitgraph.core.repository import Repository
    from splitgraph.engine import get_engine
    from splitgraph.core.engine import repository_exists

    repository, image = image_spec
    repository = Repository.from_template(repository, engine=get_engine(remote or "LOCAL"))
    if not image:
        click.echo(
            ("Repository" if repository_exists(repository) else "Postgres schema")
            + " %s will be deleted." % repository.to_schema()
        )
        if not yes:
            click.confirm("Continue? ", abort=True)

        # Don't try to "uncheckout" repositories on the registry/other remote engines
        repository.delete(uncheckout=remote is None)
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
    from splitgraph.core.repository import Repository
    from splitgraph.engine import get_engine

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

    from splitgraph.config.export import serialize_config
    from splitgraph.config import CONFIG

    click.echo(serialize_config(CONFIG, config_format, no_shielding))


@click.command(name="dump")
@click.argument("repository", type=RepositoryType(exists=True))
@click.option("--exclude-object-contents", is_flag=True, default=False)
def dump_c(repository, exclude_object_contents):
    """
    Dump a repository to SQL.
    """
    repository.dump(sys.stdout, exclude_object_contents=exclude_object_contents)


def _eval(command, args):
    from splitgraph.core.repository import Repository
    from splitgraph.engine import get_engine
    from splitgraph.core.object_manager import ObjectManager

    engine = get_engine()
    object_manager = ObjectManager(object_engine=engine, metadata_engine=engine)

    command_locals = locals().copy()
    command_locals.update({k: v for k, v in args})

    exec(command, globals(), command_locals)


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
        sgr eval 'import json; print(json.dumps(Repository\\
            .from_schema(repo_name)\\
            .images["latest"]\\
            .get_table(table_name)\\
            .table_schema))' \\
        -a repo_name my_repo -a table_name my_table

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
