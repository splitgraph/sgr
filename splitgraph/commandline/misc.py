import json
import re

import click

import splitgraph as sg


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
    Mount foreign databases as Postgres schemas.

    Uses the Postgres FDW interface to create a local Postgres schema with foreign tables that map
    to tables in other databases.

    See a given mount handler's documentation for handler-specific parameters.
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
    Delete a Postgres schema from the driver.

    If the schema represents a repository, this also deletes the repository and all of its history.

    This does not delete any physical objects that the repository depends on: use `sgr cleanup` to do that.
    """
    sg.rm(repository)


@click.command(name='init')
@click.argument('repository', type=sg.to_repository)
def init_c(repository):
    """
    Create an empty Splitgraph repository.

    This creates a single image with the hash 00000... in the new repository.
    """
    sg.init(repository)
    print("Initialized empty repository %s" % str(repository))


@click.command(name='cleanup')
def cleanup_c():
    """
    Prune unneeded objects from the driver.

    This deletes all objects from the cache that aren't required by any local repository.
    """
    deleted = sg.cleanup_objects()
    print("Deleted %d physical object(s)" % len(deleted))
