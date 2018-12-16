"""
Common internal functions used by Splitgraph commands.
"""
from splitgraph._data.common import ensure_metadata_schema
from splitgraph.commands.info import get_table
from splitgraph.commands.repository import get_current_repositories
from splitgraph.commands.tagging import set_tag
from splitgraph.connection import get_connection
from splitgraph.engine import get_engine


def set_head(repository, image):
    """Sets the HEAD pointer of a given repository to a given image. Shouldn't be used directly."""
    set_tag(repository, image, 'HEAD', force=True)


def manage_audit_triggers():
    """Does bookkeeping on audit triggers / audit table:

        * Detect tables that are being audited that don't need to be any more
          (e.g. they've been unmounted)
        * Drop audit triggers for those and delete all audit info for them
        * Set up audit triggers for new tables
    """

    engine = get_engine()
    repos_tables = [(r.to_schema(), t) for r, head in get_current_repositories()
                    for t in engine.get_all_tables(r.to_schema()) if get_table(r, t, head)]
    tracked_tables = engine.get_tracked_tables()

    to_untrack = [t for t in tracked_tables if t not in repos_tables]
    to_track = [t for t in repos_tables if t not in tracked_tables]

    if to_untrack:
        engine.untrack_tables(to_untrack)

    if to_track:
        engine.track_tables(to_track)


def manage_audit(func):
    """A decorator to be put around various Splitgraph commands that performs general admin and auditing management
    (makes sure the metadata schema exists and delete/add required audit triggers)
    """

    def wrapped(*args, **kwargs):
        try:
            ensure_metadata_schema()
            manage_audit_triggers()
            func(*args, **kwargs)
        finally:
            get_connection().commit()
            manage_audit_triggers()

    return wrapped
