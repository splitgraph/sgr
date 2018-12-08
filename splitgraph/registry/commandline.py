import click
from time import sleep

from splitgraph._data.registry import initialize_registry

@click.group()
@click.pass_context
def registry_cli(ctx):
    """
    sgr registry
    """
    pass

@registry_cli.command(name='start-server')
def start_server():
    """
    Start the registry server
    """

    print("Start server")

    try:
        while True:
            print("Server running...")
            sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("Interrupted")


@registry_cli.command(name='init')
def init_registry():
    """
    Put the driver into registry mode

    Warning: Will delete non-registry functionality
    """
    print("Command: Initialize registry")
    initialize_registry()

