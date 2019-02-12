"""
Common internal functions used by Splitgraph commands.
"""
import logging

import re
from functools import wraps
from psycopg2.sql import Identifier, SQL

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.engine import ResultShape
from splitgraph.exceptions import SplitGraphException

META_TABLES = ['images', 'tags', 'objects', 'tables', 'upstream', 'object_locations', 'object_cache_status',
               'snap_cache', 'snap_cache_misses', 'info']
_PUBLISH_PREVIEW_SIZE = 100


def set_tag(repository, image_hash, tag, force=False):
    """Internal function -- add a tag to an image."""
    engine = repository.engine
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


def manage_audit_triggers(engine):
    """Does bookkeeping on audit triggers / audit table:

        * Detect tables that are being audited that don't need to be any more
          (e.g. they've been unmounted)
        * Drop audit triggers for those and delete all audit info for them
        * Set up audit triggers for new tables
    """

    from splitgraph.core.engine import get_current_repositories
    repos_tables = [(r.to_schema(), t) for r, head in get_current_repositories(engine) if head is not None
                    for t in set(engine.get_all_tables(r.to_schema())) & set(head.get_tables())]
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

    # Make sure docstrings are passed through
    @wraps(func)
    def wrapped(self, *args, **kwargs):
        from .image import Image
        if isinstance(self, Image):
            repository = self.repository
        else:
            repository = self
        try:
            ensure_metadata_schema(repository.engine)
            manage_audit_triggers(repository.engine)
            func(self, *args, **kwargs)
        finally:
            self.engine.commit()
            manage_audit_triggers(repository.engine)

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


def _create_metadata_schema(engine):
    """
    Creates the metadata schema splitgraph_meta that stores the hash tree of schema snaps and the current tags.
    This means we can't mount anything under the schema splitgraph_meta -- much like we can't have a folder
    ".git" under Git version control...

    This all should probably be moved into some sort of a routine that runs when the whole engine is set up
    for the first time.
    """
    engine.run_sql(SQL("CREATE SCHEMA {}").format(Identifier(SPLITGRAPH_META_SCHEMA)))
    # maybe FK parent_id on image_hash. NULL there means this is the repo root.
    engine.run_sql(SQL("""CREATE TABLE {}.{} (
                    namespace       VARCHAR NOT NULL,
                    repository      VARCHAR NOT NULL,
                    image_hash      VARCHAR NOT NULL,
                    parent_id       VARCHAR,
                    created         TIMESTAMP,
                    comment         VARCHAR,
                    provenance_type VARCHAR,
                    provenance_data JSON,
                    PRIMARY KEY (namespace, repository, image_hash))""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                                Identifier("images")),
                   return_shape=None)
    engine.run_sql(SQL("""CREATE TABLE {}.{} (
                    namespace       VARCHAR NOT NULL,
                    repository      VARCHAR NOT NULL,
                    image_hash VARCHAR,
                    tag        VARCHAR,
                    PRIMARY KEY (namespace, repository, tag),
                    CONSTRAINT sh_fk FOREIGN KEY (namespace, repository, image_hash) REFERENCES {}.{})""")
                   .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("tags"),
                           Identifier(SPLITGRAPH_META_SCHEMA), Identifier("images")),
                   return_shape=None)

    # A tree of object parents. The parent of an object is not necessarily the object linked to
    # the parent commit that the object belongs to (e.g. if we imported the object from a different commit tree).
    # One object can have multiple parents (e.g. 1 SNAP and 1 DIFF).
    # The size is the on-disk (in-database) size occupied by the object table as reported by the engine
    # (not the size stored externally)
    engine.run_sql(SQL("""CREATE TABLE {}.{} (
                    object_id  VARCHAR NOT NULL,
                    namespace  VARCHAR NOT NULL,
                    size       BIGINT,
                    format     VARCHAR NOT NULL,
                    parent_id  VARCHAR)""").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("objects")),
                   return_shape=None)
    # As we don't have PKs here, we create a couple of indexes manually to make resolving tables faster.
    engine.run_sql(SQL("""CREATE INDEX idx_splitgraph_meta_objects_parent ON {}.{} (parent_id)""")
                   .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("objects")))
    engine.run_sql(SQL("""CREATE INDEX idx_splitgraph_meta_objects_object ON {}.{} (object_id)""")
                   .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("objects")))

    # Keep track of objects that have been cached locally on the engine.
    #
    # refcount:  incremented when a component requests the object to be downloaded (for materialization
    #            or a layered query). Decremented when the component has finished using the object.
    #            (maybe consider a row-level lock on this table?)
    # ready:     f if the object can't be used yet (is being downloaded), t if it can.
    # last_used: Timestamp (UTC) this object was last returned to be used in a layered query / materialization.
    engine.run_sql(SQL("""CREATE TABLE {}.{} (
                        object_id  VARCHAR NOT NULL PRIMARY KEY,
                        refcount   INTEGER,
                        ready      BOOLEAN,
                        last_used  TIMESTAMP)""")
                   .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("object_cache_status"),
                           Identifier(SPLITGRAPH_META_SCHEMA), Identifier("objects")), return_shape=None)

    # Temporary SNAP cache to speed up layered querying: maps a DIFF object to a precomputed SNAP
    # resulting from the traversal and materialization of that DIFF chain.
    engine.run_sql(SQL("""CREATE TABLE {}.{} (
                            diff_id VARCHAR NOT NULL PRIMARY KEY,
                            snap_id VARCHAR NOT NULL,
                            size    BIGINT)""")
                   .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("snap_cache")), return_shape=None)

    # Bookkeeping for the SNAP cache: records DIFF objects that are a beginning of a DIFF chain and had to be
    # returned instead of a SNAP. These are candidates for temporary SNAP generation.
    engine.run_sql(SQL("""CREATE TABLE {}.{} (
                        diff_id VARCHAR NOT NULL,
                        used_time TIMESTAMP)""")
                   .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("snap_cache_misses")), return_shape=None)
    engine.run_sql(SQL("CREATE INDEX idx_splitgraph_meta_snap_cache_misses_diff_id ON {}.{} (diff_id)")
                   .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("snap_cache_misses")))

    # Maps a given table at a given point in time to an "object ID" (either a full snapshot or a
    # delta to a previous table).
    engine.run_sql(SQL("""CREATE TABLE {}.{} (
                    namespace  VARCHAR NOT NULL,
                    repository VARCHAR NOT NULL,
                    image_hash VARCHAR NOT NULL,
                    table_name VARCHAR NOT NULL,
                    table_schema JSON,
                    object_id  VARCHAR NOT NULL,
                    PRIMARY KEY (namespace, repository, image_hash, table_name, object_id),
                    CONSTRAINT tb_fk FOREIGN KEY (namespace, repository, image_hash) REFERENCES {}.{})""")
                   .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("tables"),
                           Identifier(SPLITGRAPH_META_SCHEMA), Identifier("images")),
                   return_shape=None)

    # Keep track of what the remotes for a given repository are (by default, we create an "origin" remote
    # on initial pull)
    engine.run_sql(SQL("""CREATE TABLE {}.{} (
                    namespace          VARCHAR NOT NULL,
                    repository         VARCHAR NOT NULL,
                    remote_name        VARCHAR NOT NULL,
                    remote_namespace   VARCHAR NOT NULL,
                    remote_repository  VARCHAR NOT NULL,
                    PRIMARY KEY (namespace, repository))""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                    Identifier("upstream")),
                   return_shape=None)

    # Map objects to their locations for when they don't live on the remote or the local machine but instead
    # in S3/some FTP/HTTP server/torrent etc.
    # Lookup path to resolve an object on checkout: local -> this table -> remote (so that we don't bombard
    # the remote with queries for tables that may have been uploaded to a different place).
    engine.run_sql(SQL("""CREATE TABLE {}.{} (
                    object_id          VARCHAR NOT NULL,
                    location           VARCHAR NOT NULL,
                    protocol           VARCHAR NOT NULL,
                    PRIMARY KEY (object_id))""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                        Identifier("object_locations")),
                   return_shape=None)

    # Miscellaneous key-value information for this engine (e.g. whether uploading objects is permitted etc).
    engine.run_sql(SQL("""CREATE TABLE {}.{} (
                    key   VARCHAR NOT NULL,
                    value VARCHAR NOT NULL,
                    PRIMARY KEY (key))""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                  Identifier("info")),
                   return_shape=None)


def select(table, columns='*', where='', schema=SPLITGRAPH_META_SCHEMA):
    """
    A generic SQL SELECT constructor to simplify metadata access queries so that we don't have to repeat the same
    identifiers everywhere.

    :param table: Table to select from.
    :param columns: Columns to select as a string. WARN: concatenated directly without any formatting.
    :param where: If specified, added to the query with a "WHERE" keyword. WARN also concatenated directly.
    :param schema: Defaults to SPLITGRAPH_META_SCHEMA.
    :return: A psycopg2.sql.SQL object with the query.
    """
    query = SQL("SELECT " + columns + " FROM {}.{}").format(Identifier(schema), Identifier(table))
    if where:
        query += SQL(" WHERE " + where)
    return query


def insert(table, columns, schema=SPLITGRAPH_META_SCHEMA):
    """
    A generic SQL SELECT constructor to simplify metadata access queries so that we don't have to repeat the same
    identifiers everywhere.

    :param table: Table to select from.
    :param columns: Columns to insert as a list of strings.
    :return: A psycopg2.sql.SQL object with the query (parameterized)
    """
    query = SQL("INSERT INTO {}.{}").format(Identifier(schema), Identifier(table))
    query += SQL("(" + ",".join("{}" for _ in columns) + ")").format(*map(Identifier, columns))
    query += SQL("VALUES (" + ','.join("%s" for _ in columns) + ")")
    return query


def ensure_metadata_schema(engine):
    """Create the metadata schema if it doesn't exist"""
    if engine.run_sql(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (SPLITGRAPH_META_SCHEMA,),
            return_shape=ResultShape.ONE_ONE) is None:
        _create_metadata_schema(engine)


def aggregate_changes(query_result, initial=None):
    """Add a DIFF object to the aggregated diff result"""
    result = list(initial) if initial else [0, 0, 0]
    for kind, kind_count in query_result:
        result[kind] += kind_count
    return tuple(result)


def merged_diff(repository, table_name, path, aggregate):
    """
    Calculate the diff between two versions of the same table by combining changes in DIFF objects.
    """
    result = [] if not aggregate else (0, 0, 0)
    for image in reversed(path):
        diff_id = repository.images.by_hash(image).get_table(table_name).get_object('DIFF')
        if diff_id is None:
            # There's a SNAP entry between the two images, meaning there has been a schema change.
            # Hence we can't accumulate the DIFFs and have to resort to manual side-by-side diffing.
            return None
        if not aggregate:
            # There's only one action applied to a tuple in a single diff, so the ordering doesn't matter.
            for row in sorted(
                    repository.engine.run_sql(SQL("SELECT * FROM {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                                Identifier(diff_id)))):
                pk = row[:-2]
                action = row[-2]
                action_data = row[-1]
                result.append((pk, action, action_data))
        else:
            result = aggregate_changes(
                repository.engine.run_sql(
                    SQL("SELECT sg_action_kind, count(sg_action_kind) FROM {}.{} GROUP BY sg_action_kind")
                        .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(diff_id))), result)
    return result


def slow_diff(repository, table_name, image_1, image_2, aggregate):
    """Materialize both tables and manually diff them"""
    with repository.materialized_table(table_name, image_1) as (mp_1, table_1):
        with repository.materialized_table(table_name, image_2) as (mp_2, table_2):
            # Check both tables out at the same time since then table_2 calculation can be based
            # on table_1's snapshot.
            left = repository.engine.run_sql(SQL("SELECT * FROM {}.{}").format(Identifier(mp_1),
                                                                               Identifier(table_1)))
            right = repository.engine.run_sql(SQL("SELECT * FROM {}.{}").format(Identifier(mp_2),
                                                                                Identifier(table_2)))

    if aggregate:
        return sum(1 for r in right if r not in left), sum(1 for r in left if r not in right), 0
    # Mimic the diff format returned by the DIFF-object-accumulating function
    return [(r, 1, None) for r in left if r not in right] + [(r, 0, {}) for r in right if r not in left]


def prepare_publish_data(image, repository, include_table_previews):
    """Prepare previews and schemata for a given image for publishing to a registry."""
    schemata = {}
    previews = {}
    for table_name in image.get_tables():
        if include_table_previews:
            logging.info("Generating preview for %s...", table_name)
            with repository.materialized_table(table_name, image.image_hash) as (tmp_schema, tmp_table):
                engine = repository.engine
                schema = engine.get_full_table_schema(tmp_schema, tmp_table)
                previews[table_name] = engine.run_sql(SQL("SELECT * FROM {}.{} LIMIT %s").format(
                    Identifier(tmp_schema), Identifier(tmp_table)), (_PUBLISH_PREVIEW_SIZE,))
        else:
            schema = image.get_table(table_name).table_schema
        schemata[table_name] = [(cn, ct, pk) for _, cn, ct, pk in schema]
    return previews, schemata


def gather_sync_metadata(target, source):
    """
    Inspects two Splitgraph repositories and gathers metadata that is required to bring target up to
    date with source.

    :param target: Target Repository object
    :param source: Source repository object
    """

    target_images = {i.image_hash: i for i in target.images()}
    source_images = {i.image_hash: i for i in source.images()}

    # We assume here that none of the target image hashes have changed (are immutable) since otherwise the target
    # would have created a new images
    table_meta = []
    new_images = [i for i in source_images if i not in target_images]
    for image_hash in new_images:
        image = source_images[image_hash]
        # This is not batched but there shouldn't be that many entries here anyway.
        target.images.add(image.parent_id, image.image_hash, image.created, image.comment, image.provenance_type,
                          image.provenance_data)
        # Get the meta for all objects we'll need to fetch.
        table_meta.extend(source.engine.run_sql(
            SQL("""SELECT image_hash, table_name, table_schema, object_id FROM {0}.tables
                       WHERE namespace = %s AND repository = %s AND image_hash = %s""")
                .format(Identifier(SPLITGRAPH_META_SCHEMA)),
            (source.namespace, source.repository, image.image_hash)))
    # Get the tags too
    existing_tags = [t for s, t in target.get_all_hashes_tags()]
    tags = {t: s for s, t in source.get_all_hashes_tags() if t not in existing_tags}

    # Crawl the object tree to get the IDs and other metadata for all required objects.
    distinct_objects, object_meta = target.objects.extract_recursive_object_meta(source.objects, table_meta)

    object_locations = source.objects.get_external_object_locations(list(distinct_objects)) if distinct_objects else []
    return new_images, table_meta, object_locations, object_meta, tags


def pretty_size(size):
    size = float(size)
    power = 2 ** 10
    n = 0
    while size > power:
        size /= power
        n += 1

    return "%.2f %s" % (size, {0: '', 1: 'Ki', 2: 'Mi', 3: 'Gi', 4: 'Ti'}[n] + 'B')
