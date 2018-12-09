import json
import re

import click

import splitgraph as sg
from splitgraph.hooks.mount_handlers import get_mount_handler, get_mount_handlers

_PARAM_REGEX = re.compile('^:param\s+(?P<type>\w+\s+)?(?P<param>\w+):\s+(?P<doc>.*)$', re.MULTILINE)
# Mount handler function arguments that get parsed by other means (connection string) and aren't
# included in the generated help text.
_RESERVED_PARAMS = ['mountpoint', 'server', 'port', 'username', 'password']


@click.group(name='mount')
def mount_c():
    """
    Mount foreign databases as Postgres schemas.

    Uses the Postgres FDW interface to create a local Postgres schema with foreign tables that map
    to tables in other databases.

    See a given mount handler's documentation for handler-specific parameters.
    """


def _generate_handler_help(docstring):
    """
    Extract the long description and the parameters from a docstring
    :param docstring: Docstring
    :returns
    """
    # The handler's docstring can have \b as per Click convention to separate
    # docstring params from the rest of the help -- we do our own parsing here and
    # use it to construct a custom help string with the extra JSON-formatted (for now)
    # parameters that the mount handler takes.
    try:
        help_text, handler_params = docstring.split('\b')

        handler_help_text = "JSON-encoded dictionary of handler options:\n\n"

        formatted_params = []
        for line in handler_params.split(':param'):
            # drop empty strings...
            line = line.strip()
            if not line:
                continue
            try:
                # and params that are accounted for (e.g. connection string parsed separately)
                param_name = line[:line.index(':')]
                if param_name in _RESERVED_PARAMS:
                    continue
            except ValueError:
                continue
            formatted_params.append(line)

        handler_help_text += "\n".join(formatted_params)
        return help_text, handler_help_text
    except ValueError:
        # Ignore wrongly formatted docstrings for now
        return "", ""


def _make_mount_handler_command(handler_name):
    """Turn the mount handler function into a Click subcommand
    with help text and kwarg/connection string passing"""

    handler = get_mount_handler(handler_name)
    help_text, handler_options_help = _generate_handler_help(handler.__doc__)

    params = [click.Argument(['schema']),
              click.Option(('--connection', '-c'),
                           help='Connection string in the form username:password@server:port'),
              click.Option(('--handler-options', '-o'),
                           help=handler_options_help, default='{}')]

    def _callback(schema, connection, handler_options):
        match = re.match(r'(\S+):(\S+)@(.+):(\d+)', connection)
        # In the future, we could turn all of these options into actual Click options,
        # but then we'd also have to parse the docstring deeper to find out the types the function
        # requires, how to serialize them etc etc. Idea for a click-contrib addon perhaps?
        handler_options = json.loads(handler_options)
        handler_options.update(dict(server=match.group(3), port=int(match.group(4)),
                                    username=match.group(1), password=match.group(2)))
        sg.mount(schema, mount_handler=handler_name, handler_kwargs=handler_options)

    cmd = click.Command(handler_name, params=params, callback=_callback)
    return cmd


# Register all current mount handlers and turn them into Click subcommands.
for handler_name in get_mount_handlers():
    mount_c.add_command(_make_mount_handler_command(handler_name))
