"""
sgr commands related to sharing and downloading images.
"""

import sys
from typing import Optional

import click
from splitgraph.commandline.common import ImageType, JsonType, RepositoryType
from splitgraph.config import CONFIG, REMOTES
from splitgraph.config.config import get_from_subsection


@click.command(name="pull")
@click.argument("repository_or_image", type=ImageType(repository_exists=True, default=None))
@click.option(
    "-d",
    "--download-all",
    is_flag=True,
    help="Download all objects immediately instead of on checkout.",
)
@click.option(
    "-f",
    "--overwrite-object-meta",
    help="Overwrite metadata for existing objects",
    is_flag=True,
)
@click.option("-t", "--tags", help="Overwrite tags for pulled image/repo", is_flag=True)
def pull_c(repository_or_image, download_all, overwrite_object_meta, tags):
    """
    Pull changes / download a single image.
    """
    repository, image = repository_or_image
    repository.pull(
        download_all,
        single_image=image,
        overwrite_objects=overwrite_object_meta,
        overwrite_tags=tags,
    )


@click.command(name="clone")
@click.argument("remote_repository_or_image", type=ImageType(default=None))
@click.argument("local_repository", required=False, type=RepositoryType())
@click.option("-r", "--remote", help="Name of the remote engine")
@click.option(
    "-d",
    "--download-all",
    help="Download all objects immediately instead of on checkout.",
    default=False,
    is_flag=True,
)
@click.option(
    "-f",
    "--overwrite-object-meta",
    help="Overwrite metadata for existing objects",
    is_flag=True,
)
@click.option("-t", "--tags", help="Overwrite tags for pulled image/repo", is_flag=True)
def clone_c(
    remote_repository_or_image, local_repository, remote, download_all, overwrite_object_meta, tags
):
    """
    Clone a remote Splitgraph repository/image into a local one.

    The lookup path for the repository is governed by the ``SG_REPO_LOOKUP`` and ``SG_REPO_LOOKUP_OVERRIDE``
    config parameters and can be overridden by the command line ``--remote`` option.
    """
    from splitgraph.core.repository import Repository, clone
    from splitgraph.engine import get_engine

    remote_repository, image = remote_repository_or_image

    # If the user passed in a remote, we can inject that into the repository spec.
    # Otherwise, we have to turn the repository into a string and let clone() look up the
    # actual engine the repository lives on.
    if remote:
        remote_repository = Repository.from_template(remote_repository, engine=get_engine(remote))
    else:
        remote_repository = remote_repository.to_schema()

    clone(
        remote_repository,
        local_repository=local_repository,
        download_all=download_all,
        single_image=image,
        overwrite_objects=overwrite_object_meta,
        overwrite_tags=tags,
    )


@click.command(name="push")
@click.argument("repository_or_image", type=ImageType(default=None))
@click.argument("remote_repository", required=False, type=RepositoryType())
@click.option(
    "-r",
    "--remote",
    help="Name of the remote engine",
    type=click.Choice(REMOTES),
    default=None,
)
@click.option("-h", "--upload-handler", help="Upload handler", default="S3")
@click.option(
    "-o",
    "--upload-handler-options",
    help="Upload handler parameters",
    default="{}",
    type=JsonType(),
)
@click.option(
    "-f",
    "--overwrite-object-meta",
    help="Overwrite metadata for existing remote objects",
    is_flag=True,
)
@click.option("-t", "--tags", help="Overwrite tags for pushed image/repo", is_flag=True)
@click.option(
    "-u",
    "--reupload-objects",
    help="Reupload objects that metadata is pushed out for",
    is_flag=True,
)
def push_c(
    repository_or_image,
    remote_repository,
    remote,
    upload_handler,
    upload_handler_options,
    overwrite_object_meta,
    reupload_objects,
    tags,
):
    """
    Push images to the Splitgraph registry or another engine.

    If an image is not specified (e.g. `sgr push noaa/climate`, this will push all new images.

    By default, the repository will be pushed to a repository with the same name in the user's namespace
    (SG_NAMESPACE configuration value which defaults to the username).

    If there's a single engine registered in the config (e.g. data.splitgraph.com), it shall be the default
    destination.

    If an upstream repository/engine has been configured for this engine with `sgr upstream`,
    it will be used instead.

    Finally, if `remote_repository` or `--remote` are passed, they will take precedence.

    The actual objects will be uploaded to S3 using Minio. When pushing to another engine,
    you can choose to upload them directly by passing `--handler DB`.
    """
    repository, image = repository_or_image

    remote_repository = _determine_push_target(repository, remote_repository, remote)

    click.echo(
        "Pushing %s to %s on remote %s"
        % (repository, remote_repository, remote_repository.engine.name)
    )

    repository.push(
        remote_repository,
        handler=upload_handler,
        handler_options=upload_handler_options,
        overwrite_objects=overwrite_object_meta,
        reupload_objects=reupload_objects,
        overwrite_tags=tags,
        single_image=image,
    )


def _determine_push_target(repository, remote_repository, remote):
    """
    Create the remote Repository object we're pushing to based on all the
    parameters we've been passed

    The reason for this behaviour is to streamline out-of-the-box Splitgraph setups where
    data.splitgraph.com is the only registered engine. In that case:

     * sgr push repo: will push to myself/repo on data.splitgraph.com with S3 uploading (user's namespace).
     * sgr push noaa/climate: will push to myself/climate
     * sgr push noaa/climate noaa/climate: will explicitly push to noaa/climate (assuming the user can write
       to that repository).

    If the user registers another registry at splitgraph.mycompany.com, then they will be able to do:

    * sgr push noaa/climate -r splitgraph.mycompany.com: will push to noaa/climate

    :param repository: Local Repository, required.
    :param remote_repository: remote Repository (without the remote engine), optional.
    :param remote: Name of the remote engine/registry, optional.
    :return:
    """
    from splitgraph.core.repository import Repository
    from splitgraph.engine import get_engine

    if remote_repository and remote:
        remote_repository = Repository.from_template(remote_repository, engine=get_engine(remote))
    elif remote:
        remote_repository = _make_push_target(repository, remote)
    elif remote_repository:
        remote_repository = Repository.from_template(
            remote_repository, engine=get_engine(_get_default_remote())
        )
    else:
        remote_repository = repository.upstream or _make_push_target(
            repository, _get_default_remote()
        )
    return remote_repository


def _get_default_remote():
    if len(REMOTES) != 1:
        raise click.UsageError(
            "More than one remote registered, specify a remote explicitly with -r!"
        )
    remote = REMOTES[0]
    return remote


def _make_push_target(repository, remote):
    from splitgraph.core.repository import Repository
    from splitgraph.engine import get_engine

    namespace: Optional[str]
    try:
        namespace = get_from_subsection(CONFIG, "remotes", remote, "SG_NAMESPACE")
    except KeyError:
        namespace = None
    remote_repository = Repository.from_template(
        repository, namespace=namespace, engine=get_engine(remote)
    )
    return remote_repository


@click.command(name="upstream")
@click.argument("repository", type=RepositoryType(exists=True))
@click.option(
    "-s",
    "--set",
    "set_to",
    help="Set the upstream to a engine alias + repository",
    type=(str, RepositoryType()),
    default=("", None),
)
@click.option("-r", "--reset", help="Delete the upstream", is_flag=True, default=False)
def upstream_c(repository, set_to, reset):
    """
    Get or set the upstream for a repository.

    This shows the default repository used for pushes and pulls as well as allows to change it to a different
    remote engine and repository.

    The remote engine alias must exist in the config file.

    Examples:

    ``sgr upstream my/repo --set splitgraph.com username/repo``

    Sets the upstream for ``my/repo`` to ``username/repo`` existing on the ``splitgraph.com`` engine

    ``sgr upstream my/repo --reset``

    Removes the upstream for ``my/repo``.

    ``sgr upstream my/repo``

    Shows the current upstream for ``my/repo``.
    """
    from splitgraph.core.repository import Repository
    from splitgraph.engine import get_engine

    # surely there's a better way of finding out whether --set isn't specified
    if set_to != ("", None) and reset:
        raise click.BadParameter("Only one of --set and --reset can be specified!")

    if reset:
        if repository.upstream:
            del repository.upstream
            click.echo("Deleted upstream for %s." % repository.to_schema())
        else:
            click.echo("%s has no upstream to delete!" % repository.to_schema())
            sys.exit(1)
        return

    if set_to == ("", None):
        upstream = repository.upstream
        if upstream:
            click.echo(
                "%s is tracking %s:%s."
                % (repository.to_schema(), upstream.engine.name, upstream.to_schema())
            )
        else:
            click.echo("%s has no upstream." % repository.to_schema())
    else:
        engine, remote_repo = set_to
        try:
            remote_repo = Repository.from_template(remote_repo, engine=get_engine(engine))
        except KeyError:
            click.echo("Remote engine '%s' does not exist in the configuration file!" % engine)
            sys.exit(1)
        repository.upstream = remote_repo
        click.echo(
            "%s set to track %s:%s." % (repository.to_schema(), engine, remote_repo.to_schema())
        )
