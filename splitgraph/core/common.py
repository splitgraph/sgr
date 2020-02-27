"""
Common internal functions used by Splitgraph commands.
"""
import logging
from datetime import date, datetime, time
from decimal import Decimal
from functools import wraps
from pkgutil import get_data
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, TYPE_CHECKING, Sequence, cast

from psycopg2.sql import Composed
from psycopg2.sql import Identifier, SQL

from splitgraph.__version__ import __version__
from splitgraph.config import SPLITGRAPH_META_SCHEMA, SPLITGRAPH_API_SCHEMA
from splitgraph.engine import ResultShape
from splitgraph.exceptions import EngineInitializationError

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PsycopgEngine, PostgresEngine
    from splitgraph.core.image import Image
    from splitgraph.core.repository import Repository
    from splitgraph.core.types import TableSchema

META_TABLES = [
    "images",
    "tags",
    "objects",
    "tables",
    "upstream",
    "object_locations",
    "object_cache_status",
    "object_cache_occupancy",
    "info",
    "version",
]
OBJECT_MANAGER_TABLES = ["object_cache_status", "object_cache_occupancy"]
_PUBLISH_PREVIEW_SIZE = 100
_SPLITGRAPH_META = "resources/splitgraph_meta.sql"


def set_tag(repository: "Repository", image_hash: Optional[str], tag: str) -> None:
    """Internal function -- add a tag to an image."""
    engine = repository.engine
    engine.run_sql(
        SQL("SELECT {}.tag_image(%s,%s,%s,%s)").format(Identifier(SPLITGRAPH_API_SCHEMA)),
        (repository.namespace, repository.repository, image_hash, tag),
    )


def set_head(repository: "Repository", image: Optional[str]) -> None:
    """Sets the HEAD pointer of a given repository to a given image. Shouldn't be used directly."""
    set_tag(repository, image, "HEAD")


def manage_audit_triggers(
    engine: "PostgresEngine", object_engine: Optional["PostgresEngine"] = None
) -> None:
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

    repos_tables = [
        (r.to_schema(), t)
        for r, head in get_current_repositories(engine)
        if head is not None
        for t in set(object_engine.get_all_tables(r.to_schema())) & set(head.get_tables())
    ]
    tracked_tables = object_engine.get_tracked_tables()

    to_untrack = [t for t in tracked_tables if t not in repos_tables]
    to_track = [t for t in repos_tables if t not in tracked_tables]

    if to_untrack:
        object_engine.untrack_tables(to_untrack)

    if to_track:
        object_engine.track_tables(to_track)


def manage_audit(func: Callable) -> Callable:
    """A decorator to be put around various Splitgraph commands
    that adds/removes audit triggers for new/committed/deleted tables.
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
            repository.object_engine.commit()
            manage_audit_triggers(repository.engine, repository.object_engine)

    return wrapped


def _create_metadata_schema(engine: "PsycopgEngine") -> None:
    """
    Creates the metadata schema splitgraph_meta that stores the hash tree of schema snaps and the current tags.
    This means we can't mount anything under the schema splitgraph_meta -- much like we can't have a folder
    ".git" under Git version control...

    This all should probably be moved into some sort of a routine that runs when the whole engine is set up
    for the first time.
    """
    splitgraph_meta = get_data_safe("splitgraph", _SPLITGRAPH_META)
    engine.run_sql(splitgraph_meta.decode("utf-8"))

    engine.run_sql(
        SQL(
            """CREATE TABLE {0}.{1} (
            version VARCHAR NOT NULL,
            installed TIMESTAMP);
            INSERT INTO {0}.{1} (version, installed) VALUES (%s, now())"""
        ).format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("version")),
        (__version__,),
    )


def select(
    table: str,
    columns: str = "*",
    where: str = "",
    schema: str = SPLITGRAPH_META_SCHEMA,
    table_args: Optional[str] = None,
) -> Composed:
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
    query = SQL("SELECT " + columns) + SQL(" FROM {}.{}").format(
        Identifier(schema), Identifier(table)
    )
    if table_args:
        query += SQL(table_args)
    if where:
        query += SQL(" WHERE " + where)
    return query


def insert(table: str, columns: Sequence[str], schema: str = SPLITGRAPH_META_SCHEMA) -> Composed:
    """
    A generic SQL SELECT constructor to simplify metadata access queries so that we don't have to repeat the same
    identifiers everywhere.

    :param table: Table to select from.
    :param columns: Columns to insert as a list of strings.
    :param schema: Schema that contains the table
    :return: A psycopg2.sql.SQL object with the query (parameterized)
    """
    query = SQL("INSERT INTO {}.{}").format(Identifier(schema), Identifier(table))
    query += SQL("(" + ",".join("{}" for _ in columns) + ")").format(*map(Identifier, columns))
    query += SQL("VALUES (" + ",".join("%s" for _ in columns) + ")")
    return query


def get_metadata_schema_version(engine: "PsycopgEngine") -> Tuple[str, datetime]:
    return cast(
        Tuple[str, datetime],
        engine.run_sql(
            select("version", "version,installed") + SQL("ORDER BY installed DESC LIMIT 1"),
            return_shape=ResultShape.ONE_MANY,
        ),
    )


def ensure_metadata_schema(engine: "PsycopgEngine") -> None:
    """Create the metadata schema if it doesn't exist"""
    if (
        engine.run_sql(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
            (SPLITGRAPH_META_SCHEMA,),
            return_shape=ResultShape.ONE_ONE,
        )
        is None
    ):
        _create_metadata_schema(engine)
    else:
        schema_version, date_installed = get_metadata_schema_version(engine)

        # Currently a stub, add migration code when needed.
        logging.info(
            "Metadata schema already exists, version %s, installed on %s",
            schema_version,
            date_installed,
        )


def aggregate_changes(
    query_result: List[Tuple[int, int]], initial: Optional[Tuple[int, int, int]] = None
) -> Tuple[int, int, int]:
    """Add a changeset to the aggregated diff result"""
    result = list(initial) if initial else [0, 0, 0]
    for kind, kind_count in query_result:
        assert kind in (0, 1, 2)
        result[kind] += kind_count
    return result[0], result[1], result[2]


def slow_diff(
    repository: "Repository",
    table_name: str,
    image_1: Optional[str],
    image_2: Optional[str],
    aggregate: bool,
) -> Union[Tuple[int, int, int], List[Tuple[bool, Tuple]]]:
    """Materialize both tables and manually diff them"""
    with repository.materialized_table(table_name, image_1) as (mp_1, table_1):
        with repository.materialized_table(table_name, image_2) as (mp_2, table_2):
            # Check both tables out at the same time since then table_2 calculation can be based
            # on table_1's snapshot.
            left = repository.object_engine.run_sql(
                SQL("SELECT * FROM {}.{}").format(Identifier(mp_1), Identifier(table_1))
            )
            right = repository.object_engine.run_sql(
                SQL("SELECT * FROM {}.{}").format(Identifier(mp_2), Identifier(table_2))
            )

    if aggregate:
        return sum(1 for r in right if r not in left), sum(1 for r in left if r not in right), 0

    # Return format: list of [(False for deleted/True for inserted, full row)]
    return [(False, r) for r in left if r not in right] + [
        (True, r) for r in right if r not in left
    ]


def prepare_publish_data(
    image: "Image", repository: "Repository", include_table_previews: bool
) -> Tuple[Optional[Dict[str, List[Tuple]]], Dict[str, "TableSchema"]]:
    """Prepare previews and schemata for a given image for publishing to a registry."""
    schemata = {}
    previews = {}
    with image.query_schema() as tmp_schema:
        for table_name in image.get_tables():
            schema = image.get_table(table_name).table_schema
            schemata[table_name] = schema

            if include_table_previews:
                logging.info("Generating preview for %s...", table_name)
                engine = repository.object_engine
                previews[table_name] = engine.run_sql(
                    SQL("SELECT * FROM {}.{} LIMIT %s").format(
                        Identifier(tmp_schema), Identifier(table_name)
                    ),
                    (_PUBLISH_PREVIEW_SIZE,),
                )
    return previews, schemata


def gather_sync_metadata(
    target: "Repository", source: "Repository", overwrite_objects=False
) -> Any:
    """
    Inspects two Splitgraph repositories and gathers metadata that is required to bring target up to
    date with source.

    :param target: Target Repository object
    :param source: Source repository object
    :param overwrite_objects: If True, will return metadata for _all_ objects (not images or tables)
        in the source repository to overwrite target.

    :returns: Tuple of metadata for  new_images, new_tables, object_locations, object_meta, tags
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
        table_meta.extend(
            [
                (image_hash,) + t
                for t in source.engine.run_sql(
                    select(
                        "get_tables",
                        "table_name, table_schema, object_ids",
                        schema=SPLITGRAPH_API_SCHEMA,
                        table_args="(%s,%s,%s)",
                    ),
                    (source.namespace, source.repository, image_hash),
                )
            ]
        )
    # Get the tags too
    existing_tags = [t for s, t in target.get_all_hashes_tags()]
    tags = {t: s for s, t in source.get_all_hashes_tags() if t not in existing_tags}

    # Get objects that don't exist on the target
    table_objects = list({o for table in table_meta for o in table[3]})
    new_objects = list(set(target.objects.get_new_objects(table_objects)))

    # Ignore overwrite_objects for calculating which objects to upload the flag
    # is only for overwriting metadata).
    if new_objects:
        object_locations = source.objects.get_external_object_locations(new_objects)
    else:
        object_locations = []

    if overwrite_objects:
        new_objects = source.objects.get_objects_for_repository(source)

    if new_objects:
        object_meta = source.objects.get_object_meta(new_objects)
    else:
        object_meta = {}
    return new_images, table_meta, object_locations, object_meta, tags


def pretty_size(size: Union[int, float]) -> str:
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

    return "%.2f %s" % (size, {0: "", 1: "Ki", 2: "Mi", 3: "Gi", 4: "Ti"}[base] + "B")


def _parse_dt(string: str) -> datetime:
    _formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f",
    ]
    for fmt in _formats:
        try:
            return datetime.strptime(string, fmt)
        except ValueError:
            continue

    raise ValueError("Unknown datetime format for string %s!" % string)


def _parse_date(string: str) -> date:
    return datetime.strptime(string, "%Y-%m-%d").date()


_TYPE_MAP: Dict[str, Callable] = {
    k: cast(Callable, v)
    for ks, v in [
        (["integer", "bigint", "smallint"], int),
        (["numeric", "real", "double precision"], float),
        (["timestamp", "timestamp without time zone"], _parse_dt),
        (["date"], _parse_date),
    ]
    for k in ks
}


def adapt(value: Any, pg_type: str) -> Any:
    """
    Coerces a value with a PG type into its Python equivalent.

    :param value: Value
    :param pg_type: Postgres datatype
    :return: Coerced value.
    """
    if pg_type in _TYPE_MAP:
        return _TYPE_MAP[pg_type](value)
    return value


class Tracer:
    """
    Accumulates events and returns the times between them.
    """

    def __init__(self) -> None:
        self.start_time = datetime.now()
        self.events: List[Tuple[datetime, str]] = []

    def log(self, event: str) -> None:
        """
        Log an event at the current time
        :param event: Event name
        """
        self.events.append((datetime.now(), event))

    def get_durations(self) -> List[Tuple[str, float]]:
        """
        Return all events and durations between them.
        :return: List of (event name, time to this event from the previous event (or start))
        """
        result = []
        prev = self.start_time
        for event_time, event in self.events:
            result.append((event, (event_time - prev).total_seconds()))
            prev = event_time
        return result

    def get_total_time(self) -> float:
        """
        :return: Time from start to the final logged event.
        """
        return (self.events[-1][0] - self.start_time).total_seconds()

    def __str__(self) -> str:
        result = ""
        for event, duration in self.get_durations():
            result += "\n%s: %.3f" % (event, duration)
        result += "\nTotal: %.3f" % self.get_total_time()
        return result[1:]


def coerce_val_to_json(val: Any) -> Any:
    """
    Turn a Python value to a string/float that can be stored as JSON.
    """
    if isinstance(val, list):
        val = [coerce_val_to_json(v) for v in val]
    elif isinstance(val, tuple):
        val = tuple(coerce_val_to_json(v) for v in val)
    elif isinstance(val, dict):
        val = {k: coerce_val_to_json(v) for k, v in val.items()}
    elif isinstance(val, (Decimal, date, time, datetime)):
        # See https://www.postgresql.org/docs/11/datatype-datetime.html
        # "ISO 8601 specifies the use of uppercase letter T to separate the date and time.
        # PostgreSQL accepts that format on input, but on output it uses a space rather
        # than T, as shown above. "
        #
        # This also matches python's str().
        return str(val)
    return val


class CallbackList(list):
    """
    Used to pass around and call multiple callbacks at once.
    """

    def __call__(self, *args, **kwargs) -> None:
        for listener in self:
            listener(*args, **kwargs)


def get_data_safe(package: str, resource: str) -> bytes:
    result = get_data(package, resource)
    if result is None:
        raise EngineInitializationError(
            "Resource %s not found in package %s!" % (resource, package)
        )
    return result
