"""Default Splitgraph engine: uses PostgreSQL to store metadata and actual objects and an audit stored procedure
to track changes, as well as the Postgres FDW interface to upload/download objects to/from other Postgres engines."""
import importlib.resources
import itertools
import json
import logging
import sys
import time
from contextlib import contextmanager
from io import BytesIO, TextIOWrapper
from pathlib import PurePosixPath
from threading import get_ident
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import psycopg2
import psycopg2.extensions
from packaging.version import Version
from psycopg2 import DatabaseError
from psycopg2.errors import InvalidSchemaName, UndefinedTable
from psycopg2.extras import Json
from psycopg2.pool import AbstractConnectionPool, ThreadedConnectionPool
from psycopg2.sql import SQL, Composed, Identifier
from splitgraph.__version__ import __version__
from splitgraph.config import (
    CONFIG,
    SG_CMD_ASCII,
    SPLITGRAPH_API_SCHEMA,
    SPLITGRAPH_META_SCHEMA,
)
from splitgraph.core import server
from splitgraph.core.common import META_TABLES, ensure_metadata_schema
from splitgraph.core.sql import select
from splitgraph.core.types import TableColumn, TableSchema
from splitgraph.engine import (
    ChangeEngine,
    ObjectEngine,
    ResultShape,
    SQLEngine,
    switch_engine,
)
from splitgraph.exceptions import (
    APICompatibilityError,
    AuthAPIError,
    EngineInitializationError,
    IncompleteObjectDownloadError,
    ObjectMountingError,
    ObjectNotFoundError,
)
from splitgraph.hooks.mount_handlers import mount_postgres
from splitgraph.resources import static
from tqdm import tqdm

if TYPE_CHECKING:
    # Import the connection object under a different name as it shadows
    # the connection property otherwise
    from psycopg2._psycopg import connection as Connection

psycopg2.extensions.register_adapter(dict, Json)

_AUDIT_SCHEMA = "splitgraph_audit"
_AUDIT_TRIGGER = "audit_trigger.sql"
_PUSH_PULL = "splitgraph_api.sql"
_CSTORE = "cstore.sql"
CSTORE_SERVER = "cstore_server"
_PACKAGE = "splitgraph"
ROW_TRIGGER_NAME = "audit_trigger_row"
STM_TRIGGER_NAME = "audit_trigger_stm"
REMOTE_TMP_SCHEMA = "tmp_remote_data"
SG_UD_FLAG = "sg_ud_flag"

# Retry policy for connection errors
RETRY_DELAY = 5
RETRY_AMOUNT = 12

# Internal API data
_API_VERSION = "0.0.1"

# Limitations for SQL API that the client uses to talk to the registry. Because
# we let the client run SQL in a controlled environment on the registry, it allows
# us to batch query calls or add custom filters without adding a new API call,
# however we'd still like to limit what the client can do to make each API's
# footprint more predictable. We hence chunk some heavy queries like object
# index uploading to allow registering multiple objects at the same time whilst
# also limiting the query's maximum size.
API_MAX_QUERY_LENGTH = 261000

# In addition, some API calls (like get_object_meta) allow variadic arguments
# to decrease the number of roundtrips the client has to do -- limit this to a sane
# number.
API_MAX_VARIADIC_ARGS = 100


# PG types we can run max/min/comparisons on

PG_INDEXABLE_TYPES = [
    "bigint",
    "bigserial",
    "bit",
    "character",
    "character varying",
    "cidr",
    "date",
    "double precision",
    "inet",
    "integer",
    "money",
    "numeric",
    "real",
    "smallint",
    "smallserial",
    "serial",
    "text",
    "time",
    "time without time zone",
    "time with time zone",
    "timestamp",
    "timestamp without time zone",
    "timestamp with time zone",
]


def _quiet():
    # Don't spam connection error messages if we're in interactive mode and
    # loglevel is default (INFO/WARNING).
    return sys.stdin.isatty() and logging.getLogger().getEffectiveLevel() >= logging.INFO


def _handle_fatal(e):
    """Handle some Postgres exceptions that aren't transient."""

    if "unexpected response from login query" in str(e):
        # This one is unintuitive but is raised by the sg registry auth gateway
        # if there's been a login error (user doesn't exist or wrong password)
        raise AuthAPIError(
            "Could not open a registry connection: wrong API key or secret. "
            "Check your credentials using sgr config and make sure you've "
            "logged into the registry using sgr cloud login."
        )
    if "password authentication failed" in str(e):
        raise e


def _paginate_by_size(cur, query, argslist, max_size=API_MAX_QUERY_LENGTH):
    """Take a query and a list of args and return a list of binary strings
    (each shorter than max_size) chunking the query up into batches.
    """
    buf = b""
    for args in argslist:
        statement = cur.mogrify(query, args)
        # If max_size is <= 0, don't split the argslist up
        if 0 < max_size < len(statement):
            raise ValueError(
                ("Statement %s... exceeds maximum query size %d. " % (statement[:100], max_size))
                + "Are you trying to upload an object with a large amount of metadata?"
            )
        if 0 < max_size < len(statement) + len(buf) + 1:
            yield buf
            buf = b""
        buf += statement + b";"

    if buf:
        yield buf


T = TypeVar("T")


def chunk(sequence: Sequence[T], chunk_size: int = API_MAX_VARIADIC_ARGS) -> Iterator[List[T]]:
    curr_chunk: List[T] = []
    for i, curr in enumerate(sequence):
        curr_chunk.append(curr)
        if (i + 1) % chunk_size == 0:
            yield curr_chunk
            curr_chunk = []
    if curr_chunk:
        yield curr_chunk


def _get(d: Dict[str, Optional[str]], k: str) -> str:
    result = d.get(k)
    assert result
    return result


def get_conn_str(conn_params: Dict[str, Optional[str]]) -> str:
    server, port, username, password, dbname = (
        _get(conn_params, "SG_ENGINE_HOST"),
        _get(conn_params, "SG_ENGINE_PORT"),
        _get(conn_params, "SG_ENGINE_USER"),
        _get(conn_params, "SG_ENGINE_PWD"),
        _get(conn_params, "SG_ENGINE_DB_NAME"),
    )
    return f"postgresql://{username}:{password}@{server}:{port}/{dbname}"


class PsycopgEngine(SQLEngine):
    """Postgres SQL engine backed by a Psycopg connection."""

    def __init__(
        self,
        name: Optional[str],
        conn_params: Optional[Dict[str, Optional[str]]] = None,
        pool: Optional[AbstractConnectionPool] = None,
        autocommit: bool = False,
        registry: bool = False,
        in_fdw: bool = False,
        check_version: bool = True,
    ) -> None:
        """
        :param name: Name of the engine
        :param conn_params: Optional, dictionary of connection params as stored in the config.
        :param pool: If specified, a Psycopg connection pool to use in this engine. By default, parameters
            in conn_params are used so one of them must be specified.
        :param autocommit: If True, the engine will not use transactions for its operation.
        """
        super().__init__()

        if not conn_params and not pool:
            raise ValueError("One of conn_params/pool must be specified!")

        self.name = name
        self.autocommit = autocommit
        self.connected = False
        self.check_version = check_version
        self.registry = registry
        self.in_fdw = in_fdw

        """List of notices issued by the server during the previous execution of run_sql."""
        self.notices: List[str] = []

        if conn_params:
            self.conn_params = conn_params

            # Connection pool used by the engine, keyed by the thread ID (so one connection gets
            # claimed per thread). Usually, only one connection is used (for e.g. metadata management
            # or performing checkouts/commits). Multiple connections are used when downloading/uploading
            # objects from S3, since that is done in multiple threads and these connections are short-lived.
            server, port, username, password, dbname = (
                conn_params["SG_ENGINE_HOST"],
                conn_params["SG_ENGINE_PORT"],
                conn_params["SG_ENGINE_USER"],
                conn_params["SG_ENGINE_PWD"],
                conn_params["SG_ENGINE_DB_NAME"],
            )

            self._pool = ThreadedConnectionPool(
                minconn=0,
                maxconn=conn_params.get("SG_ENGINE_POOL", CONFIG["SG_ENGINE_POOL"]),
                host=server,
                port=int(cast(int, port)) if port is not None else None,
                user=username,
                password=password,
                dbname=dbname,
                application_name="sgr " + __version__,
            )
        else:
            self._pool = pool

    def __repr__(self) -> str:
        try:
            conn_summary = "(%s@%s:%s/%s)" % (
                self.conn_params["SG_ENGINE_USER"],
                self.conn_params["SG_ENGINE_HOST"],
                self.conn_params["SG_ENGINE_PORT"],
                self.conn_params["SG_ENGINE_DB_NAME"],
            )
        except AttributeError:
            conn_summary = " (custom connection pool)"

        return "PostgresEngine " + ((self.name + " ") if self.name else "") + conn_summary

    def commit(self) -> None:
        if self.connected:
            conn = self.connection
            conn.commit()
            self._pool.putconn(conn)

    def close_others(self) -> None:
        """
        Close and release all other connections to the connection pool.
        """
        # This is here because of the way ThreadPoolExecutor works: from within the function
        # that it calls, we have no way of knowing if we'll be called again in that thread.
        # We also can't release the connection back into the pool because that causes the
        # pool to reset it, clearing all state that we might need. Hence this has to be
        # called after the TPE has finished.

        if self.connected:
            us = get_ident()
            other_conns = [v for k, v in self._pool._used.items() if k != us]
            for c in other_conns:
                self._pool.putconn(c, close=True)

    def close(self) -> None:
        if self.connected:
            conn = self.connection
            conn.close()
            self._pool.putconn(conn)

    def rollback(self) -> None:
        if self.connected:
            if self._savepoint_stack.stack:
                self.run_sql(SQL("ROLLBACK TO ") + Identifier(self._savepoint_stack.stack.pop()))
            else:
                conn = self.connection
                conn.rollback()
                self._pool.putconn(conn)

    def lock_table(self, schema: str, table: str) -> None:
        # Allow SELECTs but not writes to a given table.
        self.run_sql(
            SQL("LOCK TABLE {}.{} IN EXCLUSIVE MODE").format(Identifier(schema), Identifier(table))
        )

    @contextmanager
    def copy_cursor(self):
        """
        Return a cursor that can be used for copy_expert operations
        """

        # Suspend the callback that terminates the connection on Ctrl+C since it doesn't work
        # with copy_expert: https://stackoverflow.com/a/37606536
        thread = psycopg2.extensions.get_wait_callback()
        try:
            psycopg2.extensions.set_wait_callback(None)
            with self.connection.cursor() as cur:
                yield cur
        finally:
            psycopg2.extensions.set_wait_callback(thread)

    @property
    def splitgraph_version(self) -> Optional[str]:
        """Returns the version of the Splitgraph library installed on the engine
        and by association the version of the engine itself."""
        if self.registry:
            raise ValueError("Engine %s is a registry, can't check version!" % self.name)
        else:
            return self._call_version_func(self.connection)

    @property
    def connection(self) -> "Connection":
        """Engine-internal Psycopg connection."""
        retries = 0
        failed = False

        def _notify():
            nonlocal failed
            print(("\033[F" if failed else "") + "Waiting for connection..." + "." * retries)
            sys.stdout.flush()
            failed = True

        while True:
            try:
                conn = self._pool.getconn(get_ident())
                if conn.closed:
                    self._pool.putconn(conn)
                    conn = self._pool.getconn(get_ident())
                # Flip the autocommit flag if needed: this is a property,
                # so changing it from False to False will fail if we're actually in a transaction.
                if conn.autocommit != self.autocommit:
                    conn.autocommit = self.autocommit

                if not self.connected:
                    # Get the engine version and warn the user if the client and the
                    # engine have mismatched versions.
                    if not self.registry and self.check_version:
                        self._check_engine_version(conn)

                    # If we're connecting to a registry, check the API
                    # version and warn about incompatibilities.
                    if self.registry:
                        self._check_api_compat(conn)

                self.connected = True
                return conn
            # If we can't connect right now, write/log a message and wait. This is slightly
            # hacky since we don't get passed whether we've been called through Click, so
            # we check if we're running interactively and then print some
            # dots instead of a scary log message (waiting until the connection actually fails)
            except psycopg2.errors.OperationalError as e:
                if retries >= RETRY_AMOUNT:
                    logging.exception(
                        "Error connecting to the engine after %d retries", RETRY_AMOUNT
                    )
                    raise
                _handle_fatal(e)
                retries += 1
                if _quiet():
                    _notify()
                else:
                    logging.error(
                        "Error connecting to the engine (%s: %s), "
                        "sleeping %.2fs and retrying (%d/%d)...",
                        str(e.__class__.__name__),
                        e,
                        RETRY_DELAY,
                        retries,
                        RETRY_AMOUNT,
                    )
                time.sleep(RETRY_DELAY)

    def _call_version_func(self, conn, func="get_splitgraph_version") -> Optional[str]:
        # Internal function to get the Splitgraph library/API version on the engine.
        # Doesn't use run_sql because it can get called as part of run_sql if it's
        # run for the first time, messing up transaction state.
        try:
            with conn.cursor() as cur:
                cur.execute(
                    SQL("SELECT {}.{}()").format(
                        Identifier(SPLITGRAPH_API_SCHEMA),
                        Identifier(func),
                    )
                )
                result = cur.fetchone()
                if result:
                    return cast(str, result[0])
                return None
        except psycopg2.errors.UndefinedFunction:
            conn.rollback()
            return None
        except psycopg2.errors.InvalidSchemaName:
            conn.rollback()
            return None

    def _check_engine_version(self, conn):
        engine_version = self._call_version_func(conn)
        if engine_version and engine_version != __version__:
            logging.warning(
                "You're running sgr %s against engine %s. "
                "It's recommended to match the versions of sgr and the "
                "engine. Either upgrade your engine or ignore this message "
                "by setting SG_CHECK_VERSION= to this engine's config.",
                __version__,
                engine_version,
            )

    def _check_api_compat(self, conn):
        # TODO here: store the version
        #   in imagemanager: check the api version, call different get_tag (more args)
        #
        remote_version = self._call_version_func(conn, "get_version")
        if not remote_version:
            return

        client = Version(_API_VERSION)
        remote = Version(remote_version)

        if client.major != remote.major:
            raise APICompatibilityError(
                "Incompatible API version between client (%s) and registry %s (%s)!"
                % (_API_VERSION, self.name, remote_version)
            )

        if client.minor != remote.minor:
            logging.warning(
                "Client has a different API version (%s) than the registry %s (%s)."
                % (_API_VERSION, self.name, remote_version)
            )

    def run_chunked_sql(
        self,
        statement: Union[bytes, Composed, str, SQL],
        arguments: Sequence[Any],
        return_shape: Optional[ResultShape] = ResultShape.MANY_MANY,
        chunk_size: int = API_MAX_VARIADIC_ARGS,
        chunk_position: int = -1,
    ) -> Any:
        """Because the Splitgraph API has a request size limitation, certain
        SQL calls with variadic arguments are going to be too long to fit that. This function
        runs an SQL query against a set of broken up arguments and returns the combined result.
        """

        # Calculate arguments
        batches: Iterable[Any]
        if chunk_position == -1:
            # The whole arguments sequence is chunked up (treated as a list of tuples)
            batches = chunk(arguments, chunk_size)
        else:
            # Treat the nth argument as a list and chunk in that instead
            subbatches = chunk(arguments[chunk_position], chunk_size)
            batches = [
                tuple(arguments[:chunk_position]) + (s,) + tuple(arguments[chunk_position + 1 :])
                for s in subbatches
            ]

        results = [self.run_sql(statement, batch, return_shape) for batch in batches]

        # Join up the results -- we can only have one row-many cols (a list of lists of singletons)
        # or many rows-many cols (a list of lists of tuples) here
        return [r for rs in results if rs for r in rs]

    def run_sql(
        self,
        statement: Union[bytes, Composed, str, SQL],
        arguments: Optional[Sequence[Any]] = None,
        return_shape: Optional[ResultShape] = ResultShape.MANY_MANY,
        named: bool = False,
    ) -> Any:

        cursor_kwargs = {"cursor_factory": psycopg2.extras.NamedTupleCursor} if named else {}
        connection = self.connection

        attempt = 0

        while True:
            with connection.cursor(**cursor_kwargs) as cur:
                try:
                    self.notices = []
                    cur.execute(statement, arguments)
                    if connection.notices:
                        self.notices = connection.notices[:]
                        del connection.notices[:]

                        if self.registry:
                            # Forward NOTICE messages from the registry back to the user
                            # (e.g. to nag them to upgrade etc).
                            for notice in self.notices:
                                logging.info("%s says: %s", self.name, notice)
                except Exception as e:
                    # Rollback the transaction (to a savepoint if we're inside the savepoint() context manager)
                    self.rollback()
                    # Go through some more common errors (like the engine not being initialized) and raise
                    # more specific Splitgraph exceptions.
                    if isinstance(e, UndefinedTable):
                        # This is not a neat way to do this but other methods involve placing wrappers around
                        # anything that sends queries to splitgraph_meta or audit schemas.
                        if _AUDIT_SCHEMA + "." in str(e):
                            raise EngineInitializationError(
                                "Audit triggers not found on the engine. Has the engine been initialized?"
                            ) from e
                        for meta_table in META_TABLES:
                            if "splitgraph_meta.%s" % meta_table in str(e):
                                raise EngineInitializationError(
                                    "splitgraph_meta not found on the engine. Has the engine been initialized?"
                                ) from e
                        if "splitgraph_meta" in str(e):
                            raise ObjectNotFoundError(e)
                    elif isinstance(e, InvalidSchemaName):
                        if "splitgraph_api" in str(e):
                            raise EngineInitializationError(
                                "splitgraph_api not found on the engine. Has the engine been initialized?"
                            ) from e
                    elif (
                        isinstance(e, psycopg2.OperationalError)
                        and "connection has been closed unexpectedly" in str(e)
                        and attempt == 0
                    ):
                        # Handle the registry closing the connection by retrying once.
                        attempt += 1
                        logging.info(f"Connection to {self.name} lost. Reconnecting...")
                        self.close()
                        connection = self.connection
                        continue

                    raise

                if cur.description is None:
                    return None

                if return_shape == ResultShape.ONE_ONE:
                    result = cur.fetchone()
                    return result[0] if result else None
                if return_shape == ResultShape.ONE_MANY:
                    return cur.fetchone()
                if return_shape == ResultShape.MANY_ONE:
                    return [c[0] for c in cur.fetchall()]
                return cur.fetchall()

    def get_primary_keys(self, schema: str, table: str) -> List[Tuple[str, str]]:
        """Inspects the Postgres information_schema to get the primary keys for a given table."""
        return cast(
            List[Tuple[str, str]],
            self.run_sql(
                SQL(
                    """SELECT a.attname, format_type(a.atttypid, a.atttypmod)
                               FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid
                                                                      AND a.attnum = ANY(i.indkey)
                               WHERE i.indrelid = '{}.{}'::regclass AND i.indisprimary"""
                ).format(Identifier(schema), Identifier(table)),
                return_shape=ResultShape.MANY_MANY,
            ),
        )

    def run_sql_batch(
        self,
        statement: Union[Composed, str],
        arguments: Any,
        schema: Optional[str] = None,
        max_size=API_MAX_QUERY_LENGTH,
    ) -> None:
        if not arguments:
            return
        with self.connection.cursor() as cur:
            try:
                if schema:
                    cur.execute("SET search_path to %s;", (schema,))

                batches = _paginate_by_size(
                    cur,
                    statement,
                    arguments,
                    max_size=API_MAX_QUERY_LENGTH,
                )
                for batch in batches:
                    cur.execute(batch)
                if schema:
                    cur.execute("SET search_path to public")
            except DatabaseError:
                self.rollback()
                raise

    def run_api_call(self, call: str, *args, schema: str = SPLITGRAPH_API_SCHEMA) -> Any:
        # When we're inside of a foreign data wrapper on the engine itself,
        # we get to avoid having to go through PostgreSQL to manage objects in
        # the engine's filesystem and call various Python procedures directly.
        # This also avoids the overhead of importing Python modules _again_
        # inside of API plpython funcs.
        if self.in_fdw:
            func = getattr(server, call)
            return func(*args)

        return self.run_sql(
            SQL("SELECT {}.{}" + "(" + ",".join(["%s"] * len(args)) + ")").format(
                Identifier(schema), Identifier(call)
            ),
            args,
            return_shape=ResultShape.ONE_ONE,
        )

    def run_api_call_batch(self, call: str, argslist, schema: str = SPLITGRAPH_API_SCHEMA):
        if self.in_fdw:
            func = getattr(server, call)
            for args in argslist:
                func(*args)
        else:
            self.run_sql_batch(
                SQL("SELECT {}.{}" + "(" + ",".join(["%s"] * len(argslist[0])) + ")").format(
                    Identifier(schema), Identifier(call)
                ),
                argslist,
            )

    def dump_table_sql(
        self,
        schema: str,
        table_name: str,
        stream: TextIOWrapper,
        columns: str = "*",
        where: str = "",
        where_args: Optional[Union[List[str], Tuple[str, str]]] = None,
        target_schema: Optional[str] = None,
        target_table: Optional[str] = None,
    ) -> None:
        target_schema = target_schema or schema
        target_table = target_table or table_name

        with self.connection.cursor() as cur:
            # Don't deserialize JSON into Python dicts (store it as a string)
            psycopg2.extras.register_default_json(cur, globally=False, loads=lambda x: x)
            psycopg2.extras.register_default_jsonb(cur, globally=False, loads=lambda x: x)
            cur.execute(select(table_name, columns, where, schema), where_args)
            if cur.rowcount == 0:
                return

            stream.write(
                SQL("INSERT INTO {}.{} VALUES \n")
                .format(Identifier(target_schema), Identifier(target_table))
                .as_string(self.connection)
            )
            stream.write(
                ",\n".join(
                    cur.mogrify("(" + ",".join(itertools.repeat("%s", len(row))) + ")", row).decode(
                        "utf-8"
                    )
                    for row in cur
                )
            )
        stream.write(";\n")

    def _admin_conn(self) -> "Connection":
        retries = 0
        failed = False

        def _notify():
            nonlocal failed
            print(("\033[F" if failed else "") + "Waiting for connection..." + "." * retries)
            sys.stdout.flush()
            failed = True

        while True:
            try:
                return psycopg2.connect(
                    dbname=self.conn_params["SG_ENGINE_POSTGRES_DB_NAME"],
                    user=self.conn_params.get("SG_ENGINE_ADMIN_USER")
                    or self.conn_params["SG_ENGINE_USER"],
                    password=self.conn_params.get("SG_ENGINE_ADMIN_PWD")
                    or self.conn_params["SG_ENGINE_PWD"],
                    host=self.conn_params["SG_ENGINE_HOST"],
                    port=self.conn_params["SG_ENGINE_PORT"],
                    application_name="sgr " + __version__,
                )
            except psycopg2.errors.DatabaseError as e:
                if retries >= RETRY_AMOUNT:
                    logging.exception(
                        "Error connecting to the engine after %d retries", RETRY_AMOUNT
                    )
                    raise
                _handle_fatal(e)
                retries += 1
                if _quiet():
                    _notify()
                else:
                    logging.error(
                        "Error connecting to the engine (%s: %s), "
                        "sleeping %.2fs and retrying (%d/%d)...",
                        str(e.__class__.__name__),
                        e,
                        RETRY_DELAY,
                        retries,
                        RETRY_AMOUNT,
                    )
                time.sleep(RETRY_DELAY)

    def initialize(
        self, skip_object_handling: bool = False, skip_create_database: bool = False
    ) -> None:
        """Create the Splitgraph Postgres database and install the audit trigger

        :param skip_object_handling: If True, skips installation of
            audit triggers and other object management routines for engines
            that don't need change tracking or checkouts.
        :param skip_create_database: Don't create the Splitgraph database"""

        logging.info("Initializing engine %r...", self)

        # Use the connection to the "postgres" database to create the actual PG_DB
        if skip_create_database:
            logging.info("Explicitly skipping create database")
        else:
            with self._admin_conn() as admin_conn:
                # CREATE DATABASE can't run inside of tx
                pg_db = self.conn_params["SG_ENGINE_DB_NAME"]

                admin_conn.autocommit = True
                with admin_conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (pg_db,))
                    if cur.fetchone() is None:
                        logging.info("Creating database %s", pg_db)
                        cur.execute("ROLLBACK")
                        cur.execute(SQL("CREATE DATABASE {}").format(Identifier(pg_db)))
                    else:
                        logging.info("Database %s already exists, skipping", pg_db)

        logging.info("Ensuring the metadata schema at %s exists...", SPLITGRAPH_META_SCHEMA)

        ensure_metadata_schema(self)

        # Install the push/pull API functions
        logging.info("Installing Splitgraph API functions...")
        push_pull = importlib.resources.read_text(static, _PUSH_PULL)
        self.run_sql(push_pull)

        if skip_object_handling:
            logging.info("Skipping installation of audit triggers/CStore as specified.")
        else:
            # Install CStore management routines
            logging.info("Installing CStore management functions...")
            push_pull = importlib.resources.read_text(static, _CSTORE)
            self.run_sql(push_pull)

            # Install the audit trigger if it doesn't exist
            if not self.schema_exists(_AUDIT_SCHEMA):
                logging.info("Installing the audit trigger...")
                audit_trigger = importlib.resources.read_text(static, _AUDIT_TRIGGER)
                self.run_sql(audit_trigger)
            else:
                logging.info("Skipping the audit trigger as it's already installed.")

            # Start up the pgcrypto extension (required for hashing fragments)
            self.run_sql("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    def delete_database(self, database: str) -> None:
        """
        Helper function to drop a database using the admin connection

        :param database: Database name to drop
        """
        with self._admin_conn() as admin_conn:
            admin_conn.autocommit = True
            with admin_conn.cursor() as cur:
                # For some reason without it we get "DROP DATABASE cannot run inside a transaction block"
                cur.execute("ROLLBACK")
                cur.execute(SQL("DROP DATABASE IF EXISTS {}").format(Identifier(database)))


class AuditTriggerChangeEngine(PsycopgEngine, ChangeEngine):
    """Change tracking based on an audit trigger stored procedure"""

    def get_tracked_tables(self) -> List[Tuple[str, str]]:
        """Return a list of tables that the audit trigger is working on."""
        return cast(
            List[Tuple[str, str]],
            self.run_sql(
                "SELECT DISTINCT event_object_schema, event_object_table "
                "FROM information_schema.triggers WHERE trigger_name IN (%s, %s)",
                (ROW_TRIGGER_NAME, STM_TRIGGER_NAME),
            ),
        )

    def track_tables(self, tables: List[Tuple[str, str]]) -> None:
        """Install the audit trigger on the required tables"""
        self.run_sql(
            SQL(";").join(
                SQL("SELECT {}.audit_table('{}.{}')").format(
                    Identifier(_AUDIT_SCHEMA), Identifier(s), Identifier(t)
                )
                for s, t in tables
            )
        )

    def untrack_tables(self, tables: List[Tuple[str, str]]) -> None:
        """Remove triggers from tables and delete their pending changes"""
        for trigger in (ROW_TRIGGER_NAME, STM_TRIGGER_NAME):
            self.run_sql(
                SQL(";").join(
                    SQL("DROP TRIGGER IF EXISTS {} ON {}.{}").format(
                        Identifier(trigger), Identifier(s), Identifier(t)
                    )
                    for s, t in tables
                )
            )
        # Delete the actual logged actions for untracked tables
        self.run_sql_batch(
            SQL("DELETE FROM {}.logged_actions WHERE schema_name = %s AND table_name = %s").format(
                Identifier(_AUDIT_SCHEMA)
            ),
            tables,
        )

    def has_pending_changes(self, schema: str) -> bool:
        """
        Return True if the tracked schema has pending changes and False if it doesn't.
        """
        return (
            self.run_sql(
                SQL("SELECT 1 FROM {}.{} WHERE schema_name = %s").format(
                    Identifier(_AUDIT_SCHEMA), Identifier("logged_actions")
                ),
                (schema,),
                return_shape=ResultShape.ONE_ONE,
            )
            is not None
        )

    def discard_pending_changes(self, schema: str, table: Optional[str] = None) -> None:
        """
        Discard recorded pending changes for a tracked schema / table
        """
        query = SQL("DELETE FROM {}.{} WHERE schema_name = %s").format(
            Identifier(_AUDIT_SCHEMA), Identifier("logged_actions")
        )

        if table:
            self.run_sql(
                query + SQL(" AND table_name = %s"), (schema, table), return_shape=ResultShape.NONE
            )
        else:
            self.run_sql(query, (schema,), return_shape=ResultShape.NONE)

    def get_pending_changes(
        self, schema: str, table: str, aggregate: bool = False
    ) -> Union[
        List[Tuple[int, int]], List[Tuple[Tuple[str, ...], bool, Dict[str, Any], Dict[str, Any]]]
    ]:
        """
        Return pending changes for a given tracked table

        :param schema: Schema the table belongs to
        :param table: Table to return changes for
        :param aggregate: Whether to aggregate changes or return them completely
        :return: If aggregate is True: List of tuples of (change_type, number of rows).
            If aggregate is False: List of (primary_key, change_type, change_data)
        """
        if aggregate:
            return [
                (_KIND[k], c)
                for k, c in self.run_sql(
                    SQL(
                        "SELECT action, count(action) FROM {}.{} "
                        "WHERE schema_name = %s AND table_name = %s GROUP BY action"
                    ).format(Identifier(_AUDIT_SCHEMA), Identifier("logged_actions")),
                    (schema, table),
                )
            ]

        ri_cols, _ = zip(*self.get_change_key(schema, table))
        result: List[Tuple[Tuple, bool, Dict, Dict]] = []
        for action, row_data, changed_fields in self.run_sql(
            SQL(
                "SELECT action, row_data, changed_fields FROM {}.{} "
                "WHERE schema_name = %s AND table_name = %s"
            ).format(Identifier(_AUDIT_SCHEMA), Identifier("logged_actions")),
            (schema, table),
        ):
            result.extend(_convert_audit_change(action, row_data, changed_fields, ri_cols))
        return result

    def get_changed_tables(self, schema: str) -> List[str]:
        """Get list of tables that have changed content"""
        return cast(
            List[str],
            self.run_sql(
                SQL(
                    """SELECT DISTINCT(table_name) FROM {}.{}
                               WHERE schema_name = %s"""
                ).format(Identifier(_AUDIT_SCHEMA), Identifier("logged_actions")),
                (schema,),
                return_shape=ResultShape.MANY_ONE,
            ),
        )


class PostgresEngine(AuditTriggerChangeEngine, ObjectEngine):
    """An implementation of the Postgres engine for Splitgraph"""

    def get_object_schema(self, object_id: str) -> "TableSchema":
        result: "TableSchema" = []

        call_result = self.run_api_call("get_object_schema", object_id)
        if isinstance(call_result, str):
            call_result = json.loads(call_result)

        for ordinal, column_name, column_type, is_pk in call_result:
            assert isinstance(ordinal, int)
            assert isinstance(column_name, str)
            assert isinstance(column_type, str)
            assert isinstance(is_pk, bool)
            result.append(TableColumn(ordinal, column_name, column_type, is_pk))

        return result

    def _set_object_schema(self, object_id: str, schema_spec: "TableSchema") -> None:
        # Drop the comments from the schema spec (not stored in the object schema file).
        schema_spec = [s[:4] for s in schema_spec]
        self.run_api_call("set_object_schema", object_id, json.dumps(schema_spec))

    def dump_object_creation(
        self,
        object_id: str,
        schema: str,
        table: Optional[str] = None,
        schema_spec: Optional["TableSchema"] = None,
        if_not_exists: bool = False,
    ) -> bytes:
        """
        Generate the SQL that remounts a foreign table pointing to a Splitgraph object.

        :param object_id: Name of the object
        :param schema: Schema to create the table in
        :param table: Name of the table to mount
        :param schema_spec: Schema of the table
        :param if_not_exists: Add IF NOT EXISTS to the DDL
        :return: SQL in bytes format.
        """
        table = table or object_id

        if not schema_spec:
            schema_spec = self.get_object_schema(object_id)
        query = SQL(
            "CREATE FOREIGN TABLE " + ("IF NOT EXISTS " if if_not_exists else "") + "{}.{} ("
        ).format(Identifier(schema), Identifier(table))
        query += SQL(",".join("{} %s " % col.pg_type for col in schema_spec)).format(
            *(Identifier(col.name) for col in schema_spec)
        )
        # foreign tables/cstore don't support PKs
        query += SQL(") SERVER {} OPTIONS (compression %s, filename %s)").format(
            Identifier(CSTORE_SERVER)
        )

        object_path = self.conn_params["SG_ENGINE_OBJECT_PATH"]
        assert object_path is not None

        with self.connection.cursor() as cur:
            return cast(
                bytes, cur.mogrify(query, ("pglz", str(PurePosixPath(object_path, object_id))))
            )

    def dump_object(self, object_id: str, stream: TextIOWrapper, schema: str) -> None:
        schema_spec = self.get_object_schema(object_id)
        stream.write(
            self.dump_object_creation(object_id, schema=schema, schema_spec=schema_spec).decode(
                "utf-8"
            )
        )
        stream.write(";\n")
        with self.connection.cursor() as cur:
            # Since we can't write into the CStore table directly, we first load the data
            # into a temporary table and then insert that data into the CStore table.
            query, args = self.dump_table_creation(
                None, "cstore_tmp_ingestion", schema_spec, temporary=True
            )
            stream.write(cur.mogrify(query, args).decode("utf-8"))
            stream.write(";\n")
            self.dump_table_sql(
                schema,
                object_id,
                stream,
                target_schema="pg_temp",
                target_table="cstore_tmp_ingestion",
            )
            stream.write(
                SQL("INSERT INTO {}.{} (SELECT * FROM pg_temp.cstore_tmp_ingestion);\n")
                .format(Identifier(schema), Identifier(object_id))
                .as_string(cur)
            )
            stream.write(
                cur.mogrify(
                    "SELECT splitgraph_api.set_object_schema(%s, %s);\n",
                    (object_id, json.dumps(schema_spec)),
                ).decode("utf-8")
            )
            stream.write("DROP TABLE pg_temp.cstore_tmp_ingestion;\n")

    def get_object_size(self, object_id: str) -> int:
        return int(self.run_api_call("get_object_size", object_id))

    def delete_objects(self, object_ids: List[str]) -> None:
        self.unmount_objects(object_ids)
        self.run_api_call_batch("delete_object_files", [(o,) for o in object_ids])

    def rename_object(self, old_object_id: str, new_object_id: str):
        self.unmount_objects([old_object_id])
        self.run_api_call("rename_object_files", old_object_id, new_object_id)
        self.mount_object(new_object_id)

    def unmount_objects(self, object_ids: List[str]) -> None:
        """Unmount objects from splitgraph_meta (this doesn't delete the physical files."""
        unmount_query = SQL(";").join(
            SQL("DROP FOREIGN TABLE IF EXISTS {}.{}").format(
                Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
            )
            for object_id in object_ids
        )
        self.run_sql(unmount_query)

    def sync_object_mounts(self) -> None:
        """Scan through local object storage and synchronize it with the foreign tables in
        splitgraph_meta (unmounting non-existing objects and mounting existing ones)."""
        object_ids = self.run_api_call("list_objects")

        mounted_objects = self.run_sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'FOREIGN'",
            (SPLITGRAPH_META_SCHEMA,),
            return_shape=ResultShape.MANY_ONE,
        )

        if mounted_objects:
            self.unmount_objects(mounted_objects)

        for object_id in object_ids:
            self.mount_object(object_id, schema_spec=self.get_object_schema(object_id))

    def mount_object(
        self,
        object_id: str,
        table: None = None,
        schema: str = SPLITGRAPH_META_SCHEMA,
        schema_spec: Optional["TableSchema"] = None,
    ) -> None:
        """
        Mount an object from local storage as a foreign table.

        :param object_id: ID of the object
        :param table: Table to mount the object into
        :param schema: Schema to mount the object into
        :param schema_spec: Schema of the object.
        """
        query = self.dump_object_creation(object_id, schema, table, schema_spec, if_not_exists=True)
        try:
            self.run_sql(query)
        except psycopg2.errors.UndefinedObject as e:
            raise ObjectMountingError(
                "Error mounting. It's possible that the object was created "
                "using an extension (e.g. PostGIS) that's not installed on this engine."
            ) from e

    def store_fragment(
        self,
        inserted: Any,
        deleted: Any,
        schema: str,
        table: str,
        source_schema: str,
        source_table: str,
        source_schema_spec: Optional[TableSchema] = None,
    ) -> None:
        temporary = schema == "pg_temp"

        schema_spec = source_schema_spec or self.get_full_table_schema(source_schema, source_table)

        # Assuming the schema_spec has the whole tuple as PK if the table has no PK.
        change_key = get_change_key(schema_spec)
        ri_cols, ri_types = zip(*change_key)
        ri_cols = list(ri_cols)
        ri_types = list(ri_types)
        non_ri_cols = [c.name for c in schema_spec if c.name not in ri_cols]
        all_cols = ri_cols + non_ri_cols

        self.create_table(
            schema,
            table,
            schema_spec=add_ud_flag_column(schema_spec),
            temporary=temporary,
        )

        # Store upserts
        # INSERT INTO target_table (sg_ud_flag, col1, col2...)
        #   (SELECT true, t.col1, t.col2, ...
        #    FROM VALUES ((pk1_1, pk1_2), (pk2_1, pk2_2)...)) v JOIN source_table t
        #    ON v.pk1 = t.pk1::pk1_type AND v.pk2::pk2_type = t.pk2...
        #    -- the cast is required since the audit trigger gives us strings for values of updated columns
        #    -- and we're intending to join those with the PKs in the original table.
        if inserted:
            if non_ri_cols:
                query = (
                    SQL("INSERT INTO {}.{} (").format(Identifier(schema), Identifier(table))
                    + SQL(",").join(Identifier(c) for c in [SG_UD_FLAG] + all_cols)
                    + SQL(")")
                    + SQL("(SELECT %s, ")
                    + SQL(",").join(SQL("t.") + Identifier(c) for c in all_cols)
                    + SQL(
                        " FROM (VALUES "
                        + ",".join(
                            itertools.repeat(
                                "(" + ",".join(itertools.repeat("%s", len(inserted[0]))) + ")",
                                len(inserted),
                            )
                        )
                        + ")"
                    )
                    + SQL(" AS v (")
                    + SQL(",").join(Identifier(c) for c in ri_cols)
                    + SQL(")")
                    + SQL("JOIN {}.{} t").format(
                        Identifier(source_schema), Identifier(source_table)
                    )
                    + SQL(" ON ")
                    + SQL(" AND ").join(
                        SQL("t.{0} = v.{0}::%s" % r).format(Identifier(c))
                        for c, r in zip(ri_cols, ri_types)
                    )
                    + SQL(")")
                )
                # Flatten the args
                args = [True] + [p for pk in inserted for p in pk]
            else:
                # If the whole tuple is the PK, there's no point joining on the actual source table
                query = (
                    SQL("INSERT INTO {}.{} (").format(Identifier(schema), Identifier(table))
                    + SQL(",").join(Identifier(c) for c in [SG_UD_FLAG] + ri_cols)
                    + SQL(")")
                    + SQL(
                        "VALUES "
                        + ",".join(
                            itertools.repeat(
                                "(" + ",".join(itertools.repeat("%s", len(inserted[0]) + 1)) + ")",
                                len(inserted),
                            )
                        )
                    )
                )
                args = [p for pk in inserted for p in [True] + list(pk)]
            self.run_sql(query, args)

        # Store the deletes
        # we don't actually have the old values here so we put NULLs (which should be compressed out).
        if deleted:
            query = (
                SQL("INSERT INTO {}.{} (").format(Identifier(schema), Identifier(table))
                + SQL(",").join(Identifier(c) for c in [SG_UD_FLAG] + ri_cols)
                + SQL(")")
                + SQL(
                    "VALUES "
                    + ",".join(
                        itertools.repeat(
                            "(" + ",".join(itertools.repeat("%s", len(deleted[0]) + 1)) + ")",
                            len(deleted),
                        )
                    )
                )
            )
            args = [p for pk in deleted for p in [False] + list(pk)]
            self.run_sql(query, args)

    def store_object(
        self,
        object_id: str,
        source_query: Union[bytes, Composed, str, SQL],
        schema_spec: TableSchema,
        source_query_args=None,
        overwrite=False,
    ) -> None:

        # The physical object storage (/var/lib/splitgraph/objects) and the actual
        # foreign tables in spligraph_meta might be desynced for a variety of reasons,
        # so we have to handle all four cases of (physical object exists/doesn't exist,
        # foreign table exists/doesn't exist).
        object_exists = bool(self.run_api_call("object_exists", object_id))

        # Try mounting the object
        if self.table_exists(SPLITGRAPH_META_SCHEMA, object_id):
            # Foreign table with that name already exists. If it does exist but there's no
            # physical object file, we'll have to write into the table anyway.
            if not object_exists:
                logging.info(
                    "Object storage, %s, mounted but no physical file, recreating",
                    object_id,
                )

            # In addition, there's a corner case where the object was mounted with different FDW
            # parameters (e.g. object path in /var/lib/splitgraph/objects has changed)
            # and we want that to be overwritten in any case, so we remount the object.
            self.unmount_objects([object_id])
            self.mount_object(object_id, schema_spec=schema_spec)
        else:
            self.mount_object(object_id, schema_spec=schema_spec)

        if object_exists:
            if overwrite:
                logging.info("Object %s already exists, will overwrite", object_id)
                self.run_sql(
                    SQL("TRUNCATE TABLE {}.{}").format(
                        Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
                    )
                )
            else:
                logging.info("Object %s already exists, skipping", object_id)
                return

        # At this point, the foreign table mounting the object exists and we've established
        # that it's a brand new table, so insert data into it.
        self.run_sql(
            SQL("INSERT INTO {}.{}").format(
                Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
            )
            + source_query,
            source_query_args,
        )

        # Also store the table schema in a file
        self._set_object_schema(object_id, schema_spec)

    @staticmethod
    def _schema_spec_to_cols(schema_spec: "TableSchema") -> Tuple[List[str], List[str]]:
        pk_cols = [p.name for p in schema_spec if p.is_pk]
        non_pk_cols = [p.name for p in schema_spec if not p.is_pk]

        if not pk_cols:
            pk_cols = [p.name for p in schema_spec if p.pg_type in PG_INDEXABLE_TYPES]
            non_pk_cols = [p.name for p in schema_spec if p.pg_type not in PG_INDEXABLE_TYPES]
        return pk_cols, non_pk_cols

    @staticmethod
    def _generate_fragment_application(
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        cols: Tuple[List[str], List[str]],
        extra_quals: Optional[Composed] = None,
    ) -> Composed:
        ri_cols, non_ri_cols = cols
        all_cols = ri_cols + non_ri_cols

        # First, delete all PKs from staging that are mentioned in the new fragment. This conveniently
        # covers both deletes and updates.

        # Also, alias tables so that we don't have to send around long strings of object IDs.
        query = (
            SQL("DELETE FROM {0}.{2} t USING {1}.{3} s").format(
                Identifier(target_schema),
                Identifier(source_schema),
                Identifier(target_table),
                Identifier(source_table),
            )
            + SQL(" WHERE ")
            + _generate_where_clause("t", ri_cols, "s")
        )

        # At this point, we can insert all rows directly since we won't have any conflicts.
        # We can also apply extra qualifiers to only insert rows that match a certain query,
        # which will result in fewer rows actually being written to the staging table.

        # INSERT INTO target_table (col1, col2...)
        #   (SELECT col1, col2, ...
        #    FROM fragment_table WHERE sg_ud_flag = true (AND optional quals))
        query += (
            SQL(";INSERT INTO {}.{} (").format(Identifier(target_schema), Identifier(target_table))
            + SQL(",").join(Identifier(c) for c in all_cols)
            + SQL(")")
            + SQL("(SELECT ")
            + SQL(",").join(Identifier(c) for c in all_cols)
            + SQL(" FROM {}.{}").format(Identifier(source_schema), Identifier(source_table))
        )
        extra_quals = [extra_quals] if extra_quals else []
        extra_quals.append(SQL("{} = true").format(Identifier(SG_UD_FLAG)))
        if extra_quals:
            query += SQL(" WHERE ") + SQL(" AND ").join(extra_quals)
        query += SQL(")")
        return query

    def apply_fragments(
        self,
        objects: List[Tuple[str, str]],
        target_schema: str,
        target_table: str,
        extra_quals: Optional[Composed] = None,
        extra_qual_args: Optional[Tuple[Any, ...]] = None,
        schema_spec: Optional["TableSchema"] = None,
        progress_every: Optional[int] = None,
    ) -> None:
        if not objects:
            return
        schema_spec = schema_spec or self.get_full_table_schema(target_schema, target_table)
        # Assume that the target table already has the required schema (including PKs)
        # and use that to generate queries to apply fragments.
        cols = self._schema_spec_to_cols(schema_spec)

        if progress_every:
            batches = list(chunk(objects, chunk_size=progress_every))
            with tqdm(total=len(objects), unit="obj") as pbar:
                for batch in batches:
                    self._apply_batch(
                        batch, target_schema, target_table, extra_quals, cols, extra_qual_args
                    )
                    pbar.update(len(batch))
                    pbar.set_postfix({"object": batch[-1][1][:10] + "..."})
        else:
            self._apply_batch(
                objects, target_schema, target_table, extra_quals, cols, extra_qual_args
            )

    def _apply_batch(
        self, objects, target_schema, target_table, extra_quals, cols, extra_qual_args
    ):
        query = SQL(";").join(
            self._generate_fragment_application(
                ss, st, target_schema, target_table, cols, extra_quals
            )
            for ss, st in objects
        )
        self.run_sql(query, (extra_qual_args * len(objects)) if extra_qual_args else None)

    def upload_objects(self, objects: List[str], remote_engine: "PostgresEngine") -> None:

        # We don't have direct access to the remote engine's storage and we also
        # can't use the old method of first creating a CStore table remotely and then
        # mounting it via FDW (because this is done on two separate connections, after
        # the table is created and committed on the first one, it can't be written into.
        #
        # So we have to do a slower method of dumping the table into binary
        # and then piping that binary into the remote engine.
        #
        # Perhaps we should drop direct uploading altogether and require people to use S3 throughout.

        pbar = tqdm(objects, unit="objs", ascii=SG_CMD_ASCII)
        for object_id in pbar:
            pbar.set_postfix(object=object_id[:10] + "...")
            schema_spec = self.get_object_schema(object_id)
            remote_engine.mount_object(object_id, schema_spec=schema_spec)

            # Truncate the remote object in case it already exists (we'll overwrite it).
            remote_engine.run_sql(
                SQL("TRUNCATE TABLE {}.{}").format(
                    Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
                )
            )

            stream = BytesIO()
            with self.copy_cursor() as cur:
                cur.copy_expert(
                    SQL("COPY {}.{} TO STDOUT WITH (FORMAT 'binary')").format(
                        Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
                    ),
                    stream,
                )
            stream.seek(0)
            with remote_engine.copy_cursor() as cur:
                cur.copy_expert(
                    SQL("COPY {}.{} FROM STDIN WITH (FORMAT 'binary')").format(
                        Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
                    ),
                    stream,
                )
            remote_engine._set_object_schema(object_id, schema_spec)
            remote_engine.commit()

    @contextmanager
    def _mount_remote_engine(self, remote_engine: "PostgresEngine") -> Iterator[str]:
        # Switch the global engine to "self" instead of LOCAL since `mount_postgres` uses the global engine
        # (we can't easily pass args into it as it's also invoked from the command line with its arguments
        # used to populate --help)
        user = remote_engine.conn_params["SG_ENGINE_USER"]
        pwd = remote_engine.conn_params["SG_ENGINE_PWD"]
        host = remote_engine.conn_params["SG_ENGINE_FDW_HOST"]
        port = remote_engine.conn_params["SG_ENGINE_FDW_PORT"]
        dbname = remote_engine.conn_params["SG_ENGINE_DB_NAME"]

        # conn_params might contain Nones for some parameters (host/port for Unix
        # socket connection, so here we have to validate that they aren't None).
        assert user is not None
        assert pwd is not None
        assert host is not None
        assert port is not None
        assert dbname is not None

        logging.info(
            "Mounting remote schema %s@%s:%s/%s/%s to %s...",
            user,
            host,
            port,
            dbname,
            SPLITGRAPH_META_SCHEMA,
            REMOTE_TMP_SCHEMA,
        )
        self.delete_schema(REMOTE_TMP_SCHEMA)
        with switch_engine(self):
            mount_postgres(
                mountpoint=REMOTE_TMP_SCHEMA,
                host=host,
                port=int(port),
                username=user,
                password=pwd,
                dbname=dbname,
                remote_schema=SPLITGRAPH_META_SCHEMA,
            )
        try:
            yield REMOTE_TMP_SCHEMA
        finally:
            self.delete_schema(REMOTE_TMP_SCHEMA)

    def download_objects(self, objects: List[str], remote_engine: "PostgresEngine") -> List[str]:
        # Instead of connecting and pushing queries to it from the Python client, we just mount the remote mountpoint
        # into a temporary space (without any checking out) and SELECT the  required data into our local tables.

        with self._mount_remote_engine(remote_engine) as remote_schema:
            downloaded_objects = []
            pbar = tqdm(objects, unit="objs", ascii=SG_CMD_ASCII)
            for object_id in pbar:
                pbar.set_postfix(object=object_id[:10] + "...")
                if not self.table_exists(remote_schema, object_id):
                    logging.error("%s not found on the remote engine!", object_id)
                    continue

                # Create the CStore table on the engine and copy the contents of the object into it.
                schema_spec = remote_engine.get_full_table_schema(SPLITGRAPH_META_SCHEMA, object_id)
                self.mount_object(object_id, schema_spec=schema_spec)
                self.copy_table(
                    remote_schema,
                    object_id,
                    SPLITGRAPH_META_SCHEMA,
                    object_id,
                    with_pk_constraints=False,
                )
                self._set_object_schema(object_id, schema_spec=schema_spec)
                downloaded_objects.append(object_id)
        if len(downloaded_objects) < len(objects):
            raise IncompleteObjectDownloadError(reason=None, successful_objects=downloaded_objects)
        return downloaded_objects

    def get_change_key(self, schema: str, table: str) -> List[Tuple[str, str]]:
        return get_change_key(self.get_full_table_schema(schema, table))


def get_change_key(schema_spec: TableSchema) -> List[Tuple[str, str]]:
    pk = [(c.name, c.pg_type) for c in schema_spec if c.is_pk]
    if pk:
        return pk

    # Only return columns that can be compared (since we'll be using them
    # for chunking)
    return [(c.name, c.pg_type) for c in schema_spec if c.pg_type in PG_INDEXABLE_TYPES]


def _split_ri_cols(
    action: str,
    row_data: Dict[str, Any],
    changed_fields: Optional[Dict[str, str]],
    ri_cols: Tuple[str, ...],
) -> Any:
    """
    :return: `(ri_data, non_ri_data)`: a tuple of 2 dictionaries:
        * `ri_data`: maps column names in `ri_cols` to values identifying the replica identity (RI) of a given tuple
        * `non_ri_data`: map of column names and values not in the RI that have been changed/updated
    """
    non_ri_data = {}
    ri_data = {}

    if action == "I":
        for column, value in row_data.items():
            if column in ri_cols:
                ri_data[column] = value
            else:
                non_ri_data[column] = value
    elif action == "D":
        for column, value in row_data.items():
            if column in ri_cols:
                ri_data[column] = value
    else:
        assert action == "U"
        for column, value in row_data.items():
            if column in ri_cols:
                ri_data[column] = value
        if changed_fields:
            for column, value in changed_fields.items():
                non_ri_data[column] = value

    return ri_data, non_ri_data


def _recalculate_disjoint_ri_cols(
    ri_cols: Tuple[str, ...],
    ri_data: Dict[str, Any],
    non_ri_data: Dict[str, Any],
    row_data: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    # If part of the PK has been updated (is in the non_ri_cols/vals), we have to instead
    # apply the update to the PK (ri_cols/vals) and recalculate the new full new tuple
    # (by applying the update to row_data).
    new_non_ri_data = {}
    row_data = row_data.copy()

    for nrc, nrv in non_ri_data.items():
        if nrc in ri_cols:
            ri_data[nrc] = nrv
        else:
            row_data[nrc] = nrv

    for col, val in row_data.items():
        if col not in ri_cols:
            new_non_ri_data[col] = val

    return ri_data, new_non_ri_data


def _convert_audit_change(
    action: str,
    row_data: Dict[str, Any],
    changed_fields: Optional[Dict[str, str]],
    ri_cols: Tuple[str, ...],
) -> List[Tuple[Tuple, bool, Dict[str, str], Dict[str, str]]]:
    """
    Converts the audit log entry into Splitgraph's internal format.

    :returns: [(pk, (True for upserted, False for deleted), (old row value if updated/deleted),
        (new row value if inserted/updated))].
        More than 1 change might be emitted from a single audit entry.
    """
    ri_data, non_ri_data = _split_ri_cols(action, row_data, changed_fields, ri_cols)
    pk_changed = any(c in ri_cols for c in non_ri_data)

    if changed_fields:
        new_row = row_data.copy()
        for key, value in changed_fields.items():
            if key not in ri_cols:
                new_row[key] = value
    else:
        new_row = row_data
    if pk_changed:
        assert action == "U"
        # If it's an update that changed the PK (e.g. the table has no replica identity so we treat the whole
        # tuple as a primary key), then we turn it into (old PK, DELETE, old row data); (new PK, INSERT, {})

        # Recalculate the new PK to be inserted + the new (full) tuple, otherwise if the whole
        # tuple hasn't been updated, we'll lose parts of the old row (see test_diff_conflation_on_commit[test_case2]).

        result = [(tuple(ri_data[c] for c in ri_cols), False, row_data, new_row)]

        ri_data, non_ri_data = _recalculate_disjoint_ri_cols(
            ri_cols, ri_data, non_ri_data, row_data
        )
        result.append((tuple(ri_data[c] for c in ri_cols), True, {}, new_row))
        return result
    if action == "U" and not non_ri_data:
        # Nothing was actually updated -- don't emit an action
        return []
    return [
        (
            tuple(ri_data[c] for c in ri_cols),
            action in ("I", "U"),
            row_data if action in ("U", "D") else {},
            new_row if action in ("I", "U") else {},
        )
    ]


_KIND = {"I": 0, "D": 1, "U": 2}


def _generate_where_clause(table: str, cols: List[str], table_2: str) -> Composed:
    return SQL(" AND ").join(
        SQL("{}.{} = {}.{}").format(
            Identifier(table), Identifier(c), Identifier(table_2), Identifier(c)
        )
        for c in cols
    )


def add_ud_flag_column(table_schema: TableSchema) -> TableSchema:
    return table_schema + [TableColumn(table_schema[-1].ordinal + 1, SG_UD_FLAG, "boolean", False)]
