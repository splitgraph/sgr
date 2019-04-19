"""
Common internal functions used by Splitgraph commands.
"""
import logging
import re
from datetime import datetime as dt, date
from decimal import Decimal
from functools import wraps

from psycopg2.sql import Identifier, SQL

from splitgraph.config import SPLITGRAPH_META_SCHEMA, SPLITGRAPH_API_SCHEMA
from splitgraph.engine import ResultShape

META_TABLES = ['images', 'tags', 'objects', 'tables', 'upstream', 'object_locations', 'object_cache_status',
               'object_cache_occupancy', 'info']
OBJECT_MANAGER_TABLES = ['object_cache_status', 'object_cache_occupancy']
_PUBLISH_PREVIEW_SIZE = 100


def set_tag(repository, image_hash, tag):
    """Internal function -- add a tag to an image."""
    engine = repository.engine
    engine.run_sql(SQL("SELECT {}.tag_image(%s,%s,%s,%s)").format(Identifier(SPLITGRAPH_API_SCHEMA)),
                   (repository.namespace, repository.repository, image_hash, tag))


def set_head(repository, image):
    """Sets the HEAD pointer of a given repository to a given image. Shouldn't be used directly."""
    set_tag(repository, image, 'HEAD')


def manage_audit_triggers(engine, object_engine=None):
    """Does bookkeeping on audit triggers / audit table:

        * Detect tables that are being audited that don't need to be any more
          (e.g. they've been unmounted)
        * Drop audit triggers for those and delete all audit info for them
        * Set up audit triggers for new tables

    :param engine: Metadata engine with information about images and their checkout state
    :param object_engine: Object engine where the checked-out table and the audit triggers are located.
    """

    object_engine = object_engine or engine

    from splitgraph.core.engine import get_current_repositories
    repos_tables = [(r.to_schema(), t) for r, head in get_current_repositories(engine) if head is not None
                    for t in set(object_engine.get_all_tables(r.to_schema())) & set(head.get_tables())]
    tracked_tables = engine.get_tracked_tables()

    to_untrack = [t for t in tracked_tables if t not in repos_tables]
    to_track = [t for t in repos_tables if t not in tracked_tables]

    if to_untrack:
        object_engine.untrack_tables(to_untrack)

    if to_track:
        object_engine.track_tables(to_track)


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
            manage_audit_triggers(repository.engine, repository.object_engine)
            return func(self, *args, **kwargs)
        finally:
            repository.commit_engines()
            manage_audit_triggers(repository.engine, repository.object_engine)

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
                    provenance_data JSONB,
                    PRIMARY KEY (namespace, repository, image_hash))""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                                Identifier("images")),
                   return_shape=None)
    engine.run_sql(SQL("""CREATE TABLE {}.{} (
                    namespace  VARCHAR NOT NULL,
                    repository VARCHAR NOT NULL,
                    image_hash VARCHAR,
                    tag        VARCHAR,
                    PRIMARY KEY (namespace, repository, tag),
                    CONSTRAINT sh_fk FOREIGN KEY (namespace, repository, image_hash) REFERENCES {}.{})""")
                   .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("tags"),
                           Identifier(SPLITGRAPH_META_SCHEMA), Identifier("images")),
                   return_shape=None)

    # Object metadata.
    # * object_id: ID of the object, calculated as sha256((insertion_hash - deletion_hash) + sha256(table_schema)
    #   + parent_id (if exists)).
    #     (-) is the vector hash subtraction operator (see `fragment_manager.Digest`)
    #     (+) is normal string concatenation
    #     table_schema is str(table_schema) of the table this fragment belongs to (as specified in the `tables` table)
    #       that encodes the column order, types, names and whether they're the primary key.
    #   The parent_id is included in the hash so that fragments with the same contents aren't portable between
    #     different parents (otherwise we can have issues where the same fragment with the same parent chain will be
    #     reused for different tables, giving wrong results: see test_diff_fragment_hashing_reused_twice).
    # * insertion_hash: the homomorphic hash of all rows inserted or upserted by this fragment (sum of sha256 hashes
    #     of every row). Can be verified by running `FragmentManager.calculate_fragment_insertion_hash`.
    # * deletion_hash: the homomorphic hash of the old values of all rows that were deleted or updated by this fragment.
    #     This can't be verified directly by looking at the fragment (since it only contains the PKs of its deleted
    #     rows): fetching all the fragments this fragment depends on is required.
    #     insertion_hash - deletion hash form the content hash of this fragment.
    #     Homomorphic hashing in this case has the property that the sum of content hashes of individual fragments
    #     is equal to the content hash of the final materialized table.
    # * namespace: Original namespace this object was created in. Only the users with write rights to a given namespace
    #   can delete/alter this object's metadata.
    # * size: the on-disk (in-database) size occupied by the object table as reported by the engine
    #   (not the size stored externally).
    # * format: Format of the object. Currently, only FRAG (splitting the table into multiple chunks that can partially
    #     overwrite each other) is supported.
    # * parent_id: The parent of an object that the object partially overwrites. When a table is materialized, the
    #     parent of an object is first copied into the staging area and then the child object is applied on top of it
    #     (essentially, a breadth-first traversal of the object tree).
    # * index: A JSON object mapping columns spanned by this object to their minimum and maximum values. Used to
    #   discard and not download at all objects that definitely don't match a given query.

    engine.run_sql(SQL("""CREATE TABLE {}.{} (
                    object_id      VARCHAR NOT NULL PRIMARY KEY,
                    namespace      VARCHAR NOT NULL,
                    size           BIGINT,
                    format         VARCHAR NOT NULL,
                    index          JSONB,
                    insertion_hash VARCHAR(64),
                    deletion_hash  VARCHAR(64),
                    parent_id  VARCHAR)""").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("objects")),
                   return_shape=None)
    # PK on object_id here (one object can't have more than 1 parent)
    engine.run_sql(SQL("""CREATE INDEX idx_splitgraph_meta_objects_parent ON {}.{} (parent_id)""")
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
                           Identifier(SPLITGRAPH_META_SCHEMA), Identifier("objects")))

    # Size of all objects cached on the engine (existing externally and downloaded for a materialization/LQ)
    engine.run_sql(SQL("""CREATE TABLE {0}.{1} (total_size BIGINT); INSERT INTO {0}.{1} VALUES (0)""")
                   .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("object_cache_occupancy")))

    # Maps a given table at a given point in time to a list of fragments that it's assembled from.
    # Only the "top" fragments are listed here: to actually materialize a given table, the parent chain of every
    # object in this list is crawled until the base object is reached.
    engine.run_sql(SQL("""CREATE TABLE {}.{} (
                    namespace  VARCHAR NOT NULL,
                    repository VARCHAR NOT NULL,
                    image_hash VARCHAR NOT NULL,
                    table_name VARCHAR NOT NULL,
                    table_schema JSONB,
                    object_ids  VARCHAR[] NOT NULL,
                    PRIMARY KEY (namespace, repository, image_hash, table_name),
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
    engine.run_sql(SQL("""CREATE TABLE {0}.{1} (
                    object_id          VARCHAR NOT NULL,
                    location           VARCHAR NOT NULL,
                    protocol           VARCHAR NOT NULL,
                    PRIMARY KEY (object_id),
                    CONSTRAINT ol_fk FOREIGN KEY (object_id) REFERENCES {0}.{2})
                    """).format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("object_locations"),
                                Identifier("objects")),
                   return_shape=None)

    # Miscellaneous key-value information for this engine (e.g. whether uploading objects is permitted etc).
    engine.run_sql(SQL("""CREATE TABLE {}.{} (
                    key   VARCHAR NOT NULL,
                    value VARCHAR NOT NULL,
                    PRIMARY KEY (key))""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                  Identifier("info")),
                   return_shape=None)


def select(table, columns='*', where='', schema=SPLITGRAPH_META_SCHEMA, table_args=None):
    """
    A generic SQL SELECT constructor to simplify metadata access queries so that we don't have to repeat the same
    identifiers everywhere.

    :param table: Table to select from.
    :param columns: Columns to select as a string. WARN: concatenated directly without any formatting.
    :param where: If specified, added to the query with a "WHERE" keyword. WARN also concatenated directly.
    :param schema: Defaults to SPLITGRAPH_META_SCHEMA.
    :param table_args: If specified, appends to the FROM clause after the table specification,
        for example, SELECT * FROM "splitgraph_api"."get_images" (%s, %s) ...
    :return: A psycopg2.sql.SQL object with the query.
    """
    query = SQL("SELECT " + columns + " FROM {}.{}").format(Identifier(schema), Identifier(table))
    if table_args:
        query += SQL(table_args)
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
    """Add a changeset to the aggregated diff result"""
    result = list(initial) if initial else [0, 0, 0]
    for kind, kind_count in query_result:
        result[kind] += kind_count
    return tuple(result)


def slow_diff(repository, table_name, image_1, image_2, aggregate):
    """Materialize both tables and manually diff them"""
    with repository.materialized_table(table_name, image_1) as (mp_1, table_1):
        with repository.materialized_table(table_name, image_2) as (mp_2, table_2):
            # Check both tables out at the same time since then table_2 calculation can be based
            # on table_1's snapshot.
            left = repository.object_engine.run_sql(SQL("SELECT * FROM {}.{}").format(Identifier(mp_1),
                                                                                      Identifier(table_1)))
            right = repository.object_engine.run_sql(SQL("SELECT * FROM {}.{}").format(Identifier(mp_2),
                                                                                       Identifier(table_2)))

    if aggregate:
        return sum(1 for r in right if r not in left), sum(1 for r in left if r not in right), 0

    # Return format: list of [(False for deleted/True for inserted, full row)]
    return [(False, r) for r in left if r not in right] + [(True, r) for r in right if r not in left]


def prepare_publish_data(image, repository, include_table_previews):
    """Prepare previews and schemata for a given image for publishing to a registry."""
    schemata = {}
    previews = {}
    for table_name in image.get_tables():
        if include_table_previews:
            logging.info("Generating preview for %s...", table_name)
            with repository.materialized_table(table_name, image.image_hash) as (tmp_schema, tmp_table):
                engine = repository.object_engine
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

    # Currently, images can't be altered once pushed out. We intend to relax this: same image hash means
    # same contents and same tables but the composition of an image can change (if we refragment a table
    # so that querying it is faster).
    table_meta = []
    new_images = []
    new_image_hashes = [i for i in source_images if i not in target_images]
    for image_hash in new_image_hashes:
        image = source_images[image_hash]
        new_images.append(image)
        # Get the meta for all objects we'll need to fetch.
        table_meta.extend([(image_hash,) + t for t in
                           source.engine.run_sql(select("get_tables", "table_name, table_schema, object_ids",
                                                        schema=SPLITGRAPH_API_SCHEMA, table_args="(%s,%s,%s)"),
                                                 (source.namespace, source.repository, image_hash))])
    # Get the tags too
    existing_tags = [t for s, t in target.get_all_hashes_tags()]
    tags = {t: s for s, t in source.get_all_hashes_tags() if t not in existing_tags}

    # Get the top objects required by all new tables (without their dependencies)
    top_objects = list({o for table in table_meta for o in table[3]})

    # Expand this list to include the objects' parents etc
    new_objects = target.objects.get_new_objects(source.objects.get_all_required_objects(top_objects))
    if new_objects:
        new_objects = list(set(new_objects))
        object_meta = source.objects.get_object_meta(new_objects)
        object_locations = source.objects.get_external_object_locations(new_objects)
    else:
        object_meta = {}
        object_locations = []
    return new_images, table_meta, object_locations, object_meta, tags


def pretty_size(size):
    """
    Converts a size in bytes to its string representation (e.g. 1024 -> 1KiB)
    :param size: Size in bytes
    """
    size = float(size)
    power = 2 ** 10
    base = 0
    while size > power:
        size /= power
        base += 1

    return "%.2f %s" % (size, {0: '', 1: 'Ki', 2: 'Mi', 3: 'Gi', 4: 'Ti'}[base] + 'B')


def _parse_dt(string):
    try:
        return dt.strptime(string, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        return dt.strptime(string, "%Y-%m-%dT%H:%M:%S")


def _parse_date(string):
    return dt.strptime(string, "%Y-%m-%d").date()


_TYPE_MAP = {k: v for ks, v in
             [(['integer', 'bigint', 'smallint'], int),
              (['numeric', 'real', 'double precision'], float),
              (['timestamp', 'timestamp without time zone'], _parse_dt),
              (['date'], _parse_date)]
             for k in ks}


def adapt(value, pg_type):
    """
    Coerces a value with a PG type into its Python equivalent. If the value is None, returns None.

    :param value: Value
    :param pg_type: Postgres datatype
    :return: Coerced value.
    """
    if value is None:
        return value
    if pg_type in _TYPE_MAP:
        return _TYPE_MAP[pg_type](value)
    return value


class Tracer:
    """
    Accumulates events and returns the times between them.
    """

    def __init__(self):
        self.start_time = dt.now()
        self.events = []

    def log(self, event):
        """
        Log an event at the current time
        :param event: Event name
        """
        self.events.append((dt.now(), event))

    def get_durations(self):
        result = []
        prev = self.start_time
        for event_time, event in self.events:
            result.append((event, (event_time - prev).total_seconds()))
            prev = event_time
        return result

    def get_total_time(self):
        return (self.events[-1][0] - self.start_time).total_seconds()

    def __str__(self):
        result = ""
        for event, duration in self.get_durations():
            result += "\n%s: %.3f" % (event, duration)
        result += "\nTotal: %.3f" % self.get_total_time()
        return result[1:]


def coerce_val_to_json(val):
    """
    Turn a Python value to a string/float that can be stored as JSON.
    """
    if isinstance(val, Decimal):
        return str(val)
    if isinstance(val, date):
        return val.isoformat()
    return val
