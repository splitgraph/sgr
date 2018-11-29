import json
import re

import click

import splitgraph as sg


@click.command(name='mount')
@click.argument('schema')
@click.option('--connection', '-c', help='Connection string in the form username:password@server:port')
@click.option('--handler', '-h', help='Mount handler, one of mongo_fdw or postgres_fdw.')
@click.option('--handler-options', '-o', help='JSON-encoded list of handler options. For postgres_fdw, use '
                                              '{"dbname": <dbname>, "remote_schema": <remote schema>, '
                                              '"tables": <tables to mount (optional)>}. For mongo_fdw, use '
                                              '{"table_name": {"db": <dbname>, "coll": <collection>, "schema": '
                                              '{"col1": "type1"...}}}',
              default='{}')
def mount_c(schema, connection, handler, handler_options):  # pylint disable=missing-docstring
    # Parse the connection string
    match = re.match(r'(\S+):(\S+)@(.+):(\d+)', connection)
    handler_options = json.loads(handler_options)
    handler_options.update(dict(server=match.group(3), port=int(match.group(4)),
                                username=match.group(1), password=match.group(2)))
    sg.mount(schema, mount_handler=handler, handler_kwargs=handler_options)


@click.command(name='unmount')
@click.argument('repository', type=sg.to_repository)
def unmount_c(repository):  # pylint disable=missing-docstring
    sg.unmount(repository)


@click.command(name='init')
@click.argument('repository', type=sg.to_repository)
def init_c(repository):  # pylint disable=missing-docstring
    sg.init(repository)
    print("Initialized empty repository %s" % str(repository))


@click.command(name='cleanup')
def cleanup_c():  # pylint disable=missing-docstring
    deleted = sg.cleanup_objects()
    print("Deleted %d physical object(s)" % len(deleted))
