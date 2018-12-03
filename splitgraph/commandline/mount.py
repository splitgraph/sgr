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
