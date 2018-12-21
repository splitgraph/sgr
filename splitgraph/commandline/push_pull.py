"""
sgr commands related to sharing and downloading images.
"""

import json
import sys

import click

import splitgraph.core.repository
import splitgraph.engine
import splitgraph.engine.postgres.engine
from splitgraph.core.repository import clone


@click.command(name='pull')
@click.argument('repository', type=splitgraph.core.repository.to_repository)
@click.option('-d', '--download-all', help='Download all objects immediately instead on checkout.')
def pull_c(repository, download_all):
    """
    Pull changes from an upstream repository.
    """
    repository.pull(download_all)


@click.command(name='clone')
@click.argument('remote_repository', type=splitgraph.core.repository.to_repository)
@click.argument('local_repository', required=False, type=splitgraph.core.repository.to_repository)
@click.option('-r', '--remote', help='Alias or full connection string for the remote engine')
@click.option('-d', '--download-all', help='Download all objects immediately instead on checkout.',
              default=False, is_flag=True)
def clone_c(remote_repository, local_repository, remote, download_all):
    """
    Clone a remote Splitgraph repository into a local one.

    The lookup path for the repository is governed by the ``SG_REPO_LOOKUP`` and ``SG_REPO_LOOKUP_OVERRIDE``
    config parameters and can be overriden by the command line ``--remote`` option.
    """

    clone(remote_repository, remote_engine=remote,
          local_repository=local_repository, download_all=download_all)


@click.command(name='push')
@click.argument('repository', type=splitgraph.core.repository.to_repository)
@click.argument('remote_repository', required=False, type=splitgraph.core.repository.to_repository)
@click.option('-r', '--remote', help='Alias or full connection string for the remote engine')
@click.option('-h', '--upload-handler', help='Upload handler', default='DB')
@click.option('-o', '--upload-handler-options', help='Upload handler parameters', default="{}")
def push_c(repository, remote_repository, remote, upload_handler, upload_handler_options):
    """
    Push changes from a local repository to the upstream.

    The actual destination is decided as follows:

      * Remote engine: ``remote`` argument (either engine alias as specified in the config or a connection string,
        then the upstream configured for the repository.

      * Remote repository: ``remote_repository`` argument, then the upstream configured for the repository, then
        the same name as the repository.

    ``-h`` and ``-o`` allow to upload the objects to somewhere else other than the external drivers. Currently,
    uploading to an S3-compatible host via Minio is supported: see :mod:`splitgraph.hooks.s3` for information
    on handler options and how to register a new upload handler.
    """
    repository.push(remote, remote_repository,
                    handler=upload_handler, handler_options=json.loads(upload_handler_options))


@click.command(name='publish')
@click.argument('repository', type=splitgraph.core.repository.to_repository)
@click.argument('tag')
@click.option('-r', '--readme', type=click.File('r'))
@click.option('--skip-provenance', is_flag=True, help="Don't include provenance in the published information.")
@click.option('--skip-previews', is_flag=True, help="Don't include table previews in the published information.")
def publish_c(repository, tag, readme, skip_provenance, skip_previews):
    """
    Publish tagged Splitgraph images to the catalog.

    Only images with a tag can be published. The image must have been pushed
    to the registry beforehand with ``sgr push``.
    """
    if readme:
        readme = readme.read()
    else:
        readme = ""
    repository.publish(tag, readme=readme, include_provenance=not skip_provenance,
                       include_table_previews=not skip_previews)


@click.command(name='upstream')
@click.argument('repository', type=splitgraph.core.repository.to_repository)
@click.option('-s', '--set', 'set_to',
              help="Set the upstream to a engine alias + repository", type=(str,
                                                                            splitgraph.core.repository.to_repository),
              default=("", None))
@click.option('-r', '--reset', help="Delete the upstream", is_flag=True, default=False)
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
    # surely there's a better way of finding out whether --set isn't specified
    if set_to != ("", None) and reset:
        raise click.BadParameter("Only one of --set and --reset can be specified!")

    if reset:
        if repository.get_upstream():
            repository.delete_upstream()
            print("Deleted upstream for %s." % repository.to_schema())
        else:
            print("%s has no upstream to delete!" % repository.to_schema())
            sys.exit(1)
        return

    if set_to == ("", None):
        upstream = repository.get_upstream()
        if upstream:
            engine, remote_repo = upstream
            print("%s is tracking %s:%s." % (repository.to_schema(), engine, remote_repo.to_schema()))
        else:
            print("%s has no upstream." % repository.to_schema())
    else:
        engine, remote_repo = set_to
        try:
            splitgraph.engine.get_remote_connection_params(engine)
        except KeyError:
            print("Remote engine '%s' does not exist in the configuration file!" % engine)
            sys.exit(1)
        repository.set_upstream(engine, remote_repo)
        print("%s set to track %s:%s." % (repository.to_schema(), engine, remote_repo.to_schema()))
