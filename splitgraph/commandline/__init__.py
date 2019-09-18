"""
Splitgraph command line client

Hooks into the API to allow management of Splitgraph repositories and images using ``sgr``.
"""

import click
import click_log
import logging

from splitgraph.commandline.cloud import cloud_c
from splitgraph.commandline.engine import engine_c
from splitgraph.commandline.example import example
from splitgraph.commandline.image_creation import checkout_c, commit_c, tag_c, import_c
from splitgraph.commandline.image_info import (
    log_c,
    diff_c,
    show_c,
    sql_c,
    status_c,
    object_c,
    objects_c,
)
from splitgraph.commandline.ingestion import csv
from splitgraph.commandline.misc import rm_c, init_c, cleanup_c, config_c, prune_c, dump_c
from splitgraph.commandline.mount import mount_c
from splitgraph.commandline.push_pull import pull_c, clone_c, push_c, publish_c, upstream_c
from splitgraph.commandline.splitfile import build_c, provenance_c, rebuild_c


logger = logging.getLogger()
click_log.basic_config(logger)


def _commit_connection(_):
    """Commit and close the PG connection when the application finishes."""
    from splitgraph.engine import get_engine

    get_engine().commit()
    get_engine().close()


@click.group(result_callback=_commit_connection)
@click_log.simple_verbosity_option(logger)
def cli():
    """Splitgraph command line client: manage and build Postgres schema images."""


# Note on the docstring format:
# * Click uses the first sentence as a short help text as a command group
# * All docstrings are in the imperative mood
#   (e.g. "Commit a Splitgraph schema" instead of "Commits a Splitgraph schema".)

# Possible extra commands:
# * sgr squash namespace/repo:image: takes an image, turns all of its objects into snapshots and creates
#   a new image (useful for publishing?)
# * sgr reset namespace/repo:image: similar to a git "soft reset": moves the HEAD pointer to the target
#   and restages all changes between the new target and the old HEAD.
#
#   It might be possible to implement this by not suspending the audit trigger on the target tables whilst
#   rerunning the checkout (applying all objects), which should put the staging area in the same state
#   and recalculate the correct pending changes for the audit table.

# Image management/creation
cli.add_command(checkout_c)
cli.add_command(commit_c)
cli.add_command(tag_c)
cli.add_command(import_c)

# Information
cli.add_command(log_c)
cli.add_command(diff_c)
cli.add_command(object_c)
cli.add_command(objects_c)
cli.add_command(show_c)
cli.add_command(sql_c)
cli.add_command(status_c)

# Engine management
cli.add_command(engine_c)

# Miscellaneous
cli.add_command(mount_c)
cli.add_command(rm_c)
cli.add_command(init_c)
cli.add_command(cleanup_c)
cli.add_command(prune_c)
cli.add_command(config_c)
cli.add_command(dump_c)

# Push/pull/sharing
cli.add_command(clone_c)
cli.add_command(pull_c)
cli.add_command(push_c)
cli.add_command(publish_c)
cli.add_command(upstream_c)

# Splitfile execution
cli.add_command(build_c)
cli.add_command(provenance_c)
cli.add_command(rebuild_c)

# Examples
cli.add_command(example)

# CSV
cli.add_command(csv)

# Cloud
cli.add_command(cloud_c)
