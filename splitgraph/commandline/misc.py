import json
import re

import click

import splitgraph as sg


# TODO maybe turn this into a group and hook the FDW handlers from splitgraph.hooks so that we can do
# sgr mount postgres my_schema instead of -h postgres.

@click.command(name='mount')
@click.argument('schema')
@click.option('--connection', '-c', help='Connection string in the form username:password@server:port')
@click.option('--handler', '-h', help='Mount handler, one of mongo_fdw, postgres_fdw or mysql_fdw.')
@click.option('--handler-options', '-o', help='JSON-encoded list of handler options. For postgres_fdw, use '
                                              '{"dbname": <dbname>, "remote_schema": <remote schema>, '
                                              '"tables": <tables to mount (optional)>}. For mongo_fdw, use '
                                              '{"table_name": {"db": <dbname>, "coll": <collection>, "schema": '
                                              '{"col1": "type1"...}}}',
              default='{}')
def mount_c(schema, connection, handler, handler_options):
    """
    Mount
    :param schema:
    :param connection:
    :param handler:
    :param handler_options:
    :return:
    """
    match = re.match(r'(\S+):(\S+)@(.+):(\d+)', connection)
    handler_options = json.loads(handler_options)
    handler_options.update(dict(server=match.group(3), port=int(match.group(4)),
                                username=match.group(1), password=match.group(2)))
    sg.mount(schema, mount_handler=handler, handler_kwargs=handler_options)


@click.command(name='rm')
@click.argument('repository', type=sg.to_repository)
def rm_c(repository):
    """
    Deletes a Postgres schema from the local driver. If the schema represents a repository, also
    deletes the repository and all of its history. This does not delete any physical objects that
    the repository depends on: use `sgr cleanup` to do that.
    """
    sg.rm(repository)


@click.command(name='init')
@click.argument('repository', type=sg.to_repository)
def init_c(repository):
    """
    Initializes an empty Splitgraph repository with a single empty image.
    """
    sg.init(repository)
    print("Initialized empty repository %s" % str(repository))


@click.command(name='cleanup')
def cleanup_c():
    """
    Deletes all physical objects from the driver that aren't required by any local repository.
    """
    deleted = sg.cleanup_objects()
    print("Deleted %d physical object(s)" % len(deleted))
