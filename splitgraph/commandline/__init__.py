"""
Splitgraph command line client

Hooks into the API to allow management of Splitgraph repositories and images using ``sgr``.
"""

import logging
import traceback

import click
import click_log
from click_log import ColorFormatter

from splitgraph.__version__ import __version__
from splitgraph.commandline.cloud import cloud_c
from splitgraph.commandline.engine import engine_c
from splitgraph.commandline.example import example
from splitgraph.commandline.image_creation import checkout_c, commit_c, tag_c, import_c, reindex_c
from splitgraph.commandline.image_info import (
    log_c,
    diff_c,
    show_c,
    sql_c,
    status_c,
    object_c,
    objects_c,
    table_c,
)
from splitgraph.commandline.ingestion import csv_group
from splitgraph.commandline.misc import (
    rm_c,
    init_c,
    cleanup_c,
    config_c,
    prune_c,
    dump_c,
    eval_c,
    upgrade_c,
)
from splitgraph.commandline.mount import mount_c
from splitgraph.commandline.push_pull import pull_c, clone_c, push_c, upstream_c
from splitgraph.commandline.splitfile import build_c, provenance_c, rebuild_c, dependents_c
from splitgraph.ingestion.singer.commandline import singer_group

logger = logging.getLogger()

# Patch click_log's handler to restore its behaviour where INFO level goes to stdout.


class ClickHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            click.echo(msg, err=record.levelno != logging.INFO)
        except Exception:
            self.handleError(record)


click_log.basic_config(logger)
logger.handlers = [ClickHandler()]
logger.handlers[0].formatter = ColorFormatter()


def _fullname(o):
    module = o.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return o.__class__.__name__
    return module + "." + o.__class__.__name__


def _patch_wrap_text():
    # Patch click's formatter to strip ```
    # etc which are used to format help text on the website.
    wrap_text = click.formatting.wrap_text

    def patched_wrap_text(*args, **kwargs):
        text = args[0]
        text = text.replace("```\n", "").replace("`", "")

        return wrap_text(text, *args[1:], **kwargs)

    click.formatting.wrap_text = patched_wrap_text


def _do_version_check():
    """Do a pre-flight version check -- by default we only do it once a day"""
    from splitgraph.cloud import AuthAPIClient
    from packaging.version import Version
    from splitgraph.config import CONFIG

    api_client = AuthAPIClient(CONFIG["SG_UPDATE_REMOTE"])
    latest = api_client.get_latest_version()

    if not latest:
        return

    if Version(latest) > Version(__version__):
        click.echo(
            "You are using sgr version %s, however version %s is available." % (__version__, latest)
        )
        click.echo("Consider upgrading by running sgr upgrade or pip install -U splitgraph.")
        click.echo("Disable this message by setting SG_UPDATE_FREQUENCY=0 in your .sgconfig.")


class WithExceptionHandler(click.Group):
    def get_command(self, ctx, cmd_name):
        # Patch only if we're invoked somehow, not if we're imported.
        _patch_wrap_text()
        return super().get_command(ctx, cmd_name)

    def invoke(self, ctx):
        from splitgraph.engine import get_engine

        engine = get_engine()
        try:
            result = super(click.Group, self).invoke(ctx)
            engine.commit()
            return result
        except Exception as exc:
            engine.rollback()
            if isinstance(
                exc,
                (click.exceptions.ClickException, click.exceptions.Abort, click.exceptions.Exit),
            ):
                raise
            # Can't seem to be able to get the click_log verbosity option
            # value so have to get it indirectly. Basically, if we're in
            # DEBUG mode, output the whole stacktrace.
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.error(traceback.format_exc())
            else:
                logger.error("%s: %s" % (_fullname(exc), exc))
            ctx.exit(code=2)
        finally:
            _do_version_check()
            engine.close()


@click.group(cls=WithExceptionHandler)
@click_log.simple_verbosity_option(logger)
@click.version_option(prog_name="sgr", version=__version__)
def cli():
    """Splitgraph command line client: manage and build Postgres schema images."""


# Note on the docstring format:
# * Click uses the first sentence as a short help text as a command group
# * All docstrings are in the imperative mood
#   (e.g. "Commit a Splitgraph schema" instead of "Commits a Splitgraph schema".)

# Note on performance:
# To avoid Python's famously slow imports which tank the performance of something like sgr --help,
# none of the CLI modules import the rest of splitgraph at toplevel apart from lightweight helpers:
# splitgraph.config and splitgraph.exceptions.
# All other imports are recommended to be pushed into the actual command definition.

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
cli.add_command(reindex_c)

# Information
cli.add_command(log_c)
cli.add_command(diff_c)
cli.add_command(object_c)
cli.add_command(objects_c)
cli.add_command(show_c)
cli.add_command(table_c)
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
cli.add_command(eval_c)
cli.add_command(upgrade_c)

# Push/pull/sharing
cli.add_command(clone_c)
cli.add_command(pull_c)
cli.add_command(push_c)
cli.add_command(upstream_c)

# Splitfile execution
cli.add_command(build_c)
cli.add_command(dependents_c)
cli.add_command(provenance_c)
cli.add_command(rebuild_c)

# Examples
cli.add_command(example)

# CSV
cli.add_command(csv_group)

# Cloud
cli.add_command(cloud_c)

# Singer
cli.add_command(singer_group)
