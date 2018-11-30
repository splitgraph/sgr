import json

import click

import splitgraph as sg


@click.command(name='pull')
@click.argument('repository', type=sg.to_repository)
@click.option('-d', '--download-all', help='Download all objects immediately instead on checkout.')
def pull_c(repository, download_all):  # pylint disable=missing-docstring
    sg.pull(repository, download_all)


@click.command(name='clone')
@click.argument('remote_repository', type=sg.to_repository)
@click.argument('local_repository', required=False, type=sg.to_repository)
@click.option('-d', '--download-all', help='Download all objects immediately instead on checkout.',
              default=False, is_flag=True)
def clone_c(remote_repository, local_repository, download_all):  # pylint disable=missing-docstring
    sg.clone(remote_repository, local_repository=local_repository, download_all=download_all)


@click.command(name='push')
@click.argument('repository', type=sg.to_repository)
@click.argument('remote', default='origin')  # origin (must be specified in the config or remembered by the repo)
@click.argument('remote_repository', required=False, type=sg.to_repository)
@click.option('-h', '--upload-handler', help='Where to upload objects (FILE or DB for the remote itself)', default='DB')
@click.option('-o', '--upload-handler-options', help="""For FILE, e.g. '{"path": /mnt/sgobjects}'""", default="{}")
def push_c(repository, remote, remote_repository,
           upload_handler, upload_handler_options):  # pylint disable=missing-docstring
    if not remote_repository:
        # Get actual connection string and remote repository
        remote_info = sg.get_upstream(repository, remote)
        if not remote_info:
            raise sg.SplitGraphException("No remote found for %s!" % str(repository))
        remote, remote_repository = remote_info
    else:
        remote = sg.serialize_connection_string(*sg.get_remote_connection_params(remote))
    sg.push(repository, remote, remote_repository, handler=upload_handler,
            handler_options=json.loads(upload_handler_options))


@click.command(name='publish')
@click.argument('repository', type=sg.to_repository)
@click.argument('tag')
@click.option('-r', '--readme', type=click.File('r'))
@click.option('--skip-provenance', is_flag=True, help='Don''t include provenance in the published information.')
@click.option('--skip-previews', is_flag=True, help='Don''t include table previews in the published information.')
def publish_c(repository, tag, readme, skip_provenance, skip_previews):  # pylint disable=missing-docstring
    if readme:
        readme = readme.read()
    else:
        readme = ""
    sg.publish(repository, tag, readme=readme, include_provenance=not skip_provenance,
               include_table_previews=not skip_previews)
