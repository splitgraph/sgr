"""
sgr commands related to mounting databases via Postgres FDW
"""

from typing import Tuple, Type

import click
from click.core import Command

from splitgraph.commandline.common import JsonType
from splitgraph.hooks.data_source import get_data_source, get_data_sources
from splitgraph.hooks.data_source.fdw import ForeignDataWrapperDataSource
from splitgraph.hooks.mount_handlers import mount


@click.group(name="mount")
def mount_c():
    """
    Mount foreign databases as Postgres schemas.

    Uses the Postgres FDW interface to create a local Postgres schema with foreign tables that map
    to tables in other databases.

    See a given mount handler's documentation for handler-specific parameters.
    """


def _generate_handler_help(handler: Type[ForeignDataWrapperDataSource]) -> Tuple[str, str]:
    """
    Extract the long description and the parameters from a handler's class
    """
    handler_help = (
        handler.commandline_help or handler.get_name() + "\n\n" + handler.get_description()
    )

    if handler.commandline_kwargs_help:
        handler_kwargs_help = (
            "JSON-encoded dictionary or @filename.json with handler options:\n\n"
            + handler.commandline_kwargs_help
        )
    else:
        handler_kwargs_help = "JSON-encoded dictionary or @filename.json with handler options"

    return handler_help, handler_kwargs_help


def _make_mount_handler_command(
    handler_name: str, handler: Type[ForeignDataWrapperDataSource]
) -> Command:
    """Turn the mount handler function into a Click subcommand
    with help text and kwarg/connection string passing"""

    help_text, handler_options_help = _generate_handler_help(handler)

    params = [
        click.Argument(["schema"]),
        click.Option(
            ["--connection", "-c"],
            help="Connection string in the form username:password@server:port",
        ),
        click.Option(
            ["--handler-options", "-o"], help=handler_options_help, default="{}", type=JsonType()
        ),
    ]
    from splitgraph.core.output import conn_string_to_dict

    def _callback(schema, connection, handler_options):
        handler_options.update(conn_string_to_dict(connection))
        mount(schema, mount_handler=handler_name, handler_kwargs=handler_options)

    cmd = click.Command(handler_name, params=params, callback=_callback, help=help_text)
    return cmd


# Register all current mount handlers and turn them into Click subcommands.
for _handler_name in get_data_sources():
    _handler = get_data_source(_handler_name)
    if not _handler.supports_mount:
        continue
    assert issubclass(_handler, ForeignDataWrapperDataSource)
    mount_c.add_command(_make_mount_handler_command(_handler_name, _handler))
