"""
Common internal functions used by Splitgraph commands.
"""
import re

from psycopg2.sql import Identifier, SQL

from splitgraph._data.common import ensure_metadata_schema, select, insert
from splitgraph.commands.repository import get_current_repositories
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.engine import get_engine, ResultShape
from splitgraph.exceptions import SplitGraphException


def set_tag(repository, image_hash, tag, force=False):
    engine = get_engine()
    if engine.run_sql(select("tags", "1", "namespace = %s AND repository = %s AND tag = %s"),
                      (repository.namespace, repository.repository, tag),
                      return_shape=ResultShape.ONE_ONE) is None:
        engine.run_sql(insert("tags", ("image_hash", "namespace", "repository", "tag")),
                       (image_hash, repository.namespace, repository.repository, tag),
                       return_shape=None)
    else:
        if force:
            engine.run_sql(SQL("UPDATE {}.tags SET image_hash = %s "
                               "WHERE namespace = %s AND repository = %s AND tag = %s")
                           .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                           (image_hash, repository.namespace, repository.repository, tag),
                           return_shape=None)
        else:
            raise SplitGraphException("Tag %s already exists in %s!" % (tag, repository.to_schema()))


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
    repos_tables = [(r.to_schema(), t) for r, head in get_current_repositories() if head is not None
                    for t in set(engine.get_all_tables(r.to_schema())) & set(r.get_image(head).get_tables())]
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
            get_engine().commit()
            manage_audit_triggers()

    return wrapped


def parse_connection_string(conn_string):
    """
    :return: a tuple (server, port, username, password, dbname)
    """
    match = re.match(r'(\S+):(\S+)@(.+):(\d+)/(\S+)', conn_string)
    if not match:
        raise ValueError("Connection string doesn't match the format!")
    return match.group(3), int(match.group(4)), match.group(1), match.group(2), match.group(5)


def serialize_connection_string(server, port, username, password, dbname):
    """
    Serializes a tuple into a Splitgraph engine connection string.
    """
    return '%s:%s@%s:%s/%s' % (username, password, server, port, dbname)
