import json

import click

import splitgraph as sg


@click.command(name='pull')
@click.argument('repository', type=sg.to_repository)
@click.option('-d', '--download-all', help='Download all objects immediately instead on checkout.')
def pull_c(repository, download_all):
    """
    Synchronises the state of a locally cloned repository with its upstream counterpart.
    """
    sg.pull(repository, download_all)


@click.command(name='clone')
@click.argument('remote_repository', type=sg.to_repository)
@click.argument('local_repository', required=False, type=sg.to_repository)
@click.option('-d', '--download-all', help='Download all objects immediately instead on checkout.',
              default=False, is_flag=True)
def clone_c(remote_repository, local_repository, download_all):
    """
    Clones a remote Splitgraph repository into a local one. The lookup path for the repository
    is governed by the SG_REPO_LOOKUP and SG_REPO_LOOKUP_OVERRIDE config parameters.
    """
    sg.clone(remote_repository, local_repository=local_repository, download_all=download_all)


@click.command(name='push')
@click.argument('repository', type=sg.to_repository)
@click.argument('remote', required=False)
@click.argument('remote_repository', required=False, type=sg.to_repository)
@click.option('-h', '--upload-handler', help='Where to upload objects (FILE or DB for the remote itself)', default='DB')
@click.option('-o', '--upload-handler-options', help="""For FILE, e.g. '{"path": /mnt/sgobjects}'""", default="{}")
def push_c(repository, remote, remote_repository,
           upload_handler, upload_handler_options):
    """
    Pushes a repository to its upstream. The actual destination is decided as follows:

    Remote driver: `remote` argument (either driver alias as specified in the config or a connection string,
    then the upstream configured for the repository.

    Remote repository: `remote_repository` argument, then the upstream configured for the repository, then
    the same name as the repository.
    """

    # Maybe we should swap remote_repository and remote (or make -r an option) since people might push
    # to different repos (e.g. their own forks) more often than they will to other drivers?
    # It feels as if we'd also want to have some commandline entry points for managing the upstream/drivers
    sg.push(repository, remote, remote_repository, handler=upload_handler,
            handler_options=json.loads(upload_handler_options))


@click.command(name='publish')
@click.argument('repository', type=sg.to_repository)
@click.argument('tag')
@click.option('-r', '--readme', type=click.File('r'))
@click.option('--skip-provenance', is_flag=True, help='Don''t include provenance in the published information.')
@click.option('--skip-previews', is_flag=True, help='Don''t include table previews in the published information.')
def publish_c(repository, tag, readme, skip_provenance, skip_previews):
    """
    Indexes a Splitgraph image pointed to by a tag and makes it available in the catalog.

    The image must have been pushed to the registry beforehand.
    """
    if readme:
        readme = readme.read()
    else:
        readme = ""
    sg.publish(repository, tag, readme=readme, include_provenance=not skip_provenance,
               include_table_previews=not skip_previews)
