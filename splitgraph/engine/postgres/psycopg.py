import importlib.resources
import itertools
import logging
import sys
import time
from contextlib import contextmanager
from io import TextIOWrapper
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
from packaging.version import Version
from psycopg2 import DatabaseError
from psycopg2.errors import InvalidSchemaName, UndefinedTable
from psycopg2.extras import Json
from psycopg2.pool import AbstractConnectionPool, ThreadedConnectionPool
from psycopg2.sql import SQL, Composed, Identifier

from splitgraph.__version__ import __version__
from splitgraph.config import CONFIG, SPLITGRAPH_API_SCHEMA, SPLITGRAPH_META_SCHEMA
from splitgraph.core import server
from splitgraph.core.migration import META_TABLES, ensure_metadata_schema
from splitgraph.core.sql.queries import select
from splitgraph.engine import ResultShape
from splitgraph.engine.base import SQLEngine
from splitgraph.exceptions import (
    APICompatibilityError,
    AuthAPIError,
    EngineInitializationError,
    ObjectNotFoundError,
)
from splitgraph.resources import static

if TYPE_CHECKING:
    # Import the connection object under a different name as it shadows
    # the connection property otherwise
    from psycopg2._psycopg import connection as Connection


psycopg2.extensions.register_adapter(dict, Json)

_AUDIT_SCHEMA = "splitgraph_audit"
_AUDIT_TRIGGER = "audit_trigger.sql"
_PUSH_PULL = "splitgraph_api.sql"
_CSTORE = "cstore.sql"

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
                        and (
                            "connection has been closed unexpectedly" in str(e)
                            or "server closed the connection unexpectedly" in str(e)
                        )
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
                """SELECT c.column_name, c.data_type
FROM information_schema.table_constraints tc 
JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
WHERE constraint_type = 'PRIMARY KEY'
AND tc.constraint_schema = %s
AND tc.table_name = %s
""",
                (schema, table),
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
