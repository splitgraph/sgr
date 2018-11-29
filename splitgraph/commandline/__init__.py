"""
Splitgraph command line client

Hooks into the API to allow management of Splitgraph repositories and images using `sgr`.
"""

import click

import splitgraph as sg
from splitgraph.commandline.image_creation import checkout_c, commit_c, tag_c, import_c
from splitgraph.commandline.image_info import log_c, diff_c, show_c, sql_c, status_c
from splitgraph.commandline.misc import mount_c, unmount_c, init_c, cleanup_c
from splitgraph.commandline.push_pull import pull_c, clone_c, push_c, publish_c
from splitgraph.commandline.splitfile import build_c, provenance_c, rebuild_c


def _commit_connection(result):
    """Commit and close the PG connection when the application finishes."""
    conn = sg.get_connection()
    if conn:
        conn.commit()
        conn.close()


@click.group(result_callback=_commit_connection)
def cli():
    """Toplevel click command group to allow us to invoke e.g. "sgr checkout" / "sgr commit" etc."""


cli.add_command(status_c)
cli.add_command(log_c)
cli.add_command(mount_c)
cli.add_command(unmount_c)
cli.add_command(checkout_c)
cli.add_command(diff_c)
cli.add_command(commit_c)
cli.add_command(show_c)
cli.add_command(build_c)
cli.add_command(sql_c)
cli.add_command(init_c)
cli.add_command(clone_c)
cli.add_command(pull_c)
cli.add_command(push_c)
cli.add_command(tag_c)
cli.add_command(import_c)
cli.add_command(cleanup_c)
cli.add_command(provenance_c)
cli.add_command(rebuild_c)
cli.add_command(publish_c)
