"""
Public API for managing images in a Splitgraph repository.
"""

import itertools
import logging
import re
from contextlib import contextmanager
from datetime import datetime
from io import TextIOWrapper
from random import getrandbits
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union, Set, Sequence, cast

from psycopg2.sql import Composed
from psycopg2.sql import SQL, Identifier

from splitgraph.config import (
    SPLITGRAPH_META_SCHEMA,
    SPLITGRAPH_API_SCHEMA,
    FDW_CLASS,
    get_singleton,
    CONFIG,
)
from splitgraph.core.fragment_manager import ExtraIndexInfo
from splitgraph.core.image import Image
from splitgraph.core.image_manager import ImageManager
from splitgraph.core.sql import validate_import_sql, select, insert
from splitgraph.core.table import Table
from splitgraph.core.types import TableSchema
from splitgraph.engine.postgres.engine import PostgresEngine
from splitgraph.exceptions import (
    CheckoutError,
    TableNotFoundError,
    IncompleteObjectUploadError,
    RepositoryNotFoundError,
)
from .common import (
    manage_audit_triggers,
    set_head,
    manage_audit,
    aggregate_changes,
    slow_diff,
    gather_sync_metadata,
    set_tags_batch,
    get_temporary_table_id,
)
from .output import pluralise
from .engine import lookup_repository, get_engine
from .object_manager import ObjectManager
from ..engine import ResultShape


class Repository:
    """
    Splitgraph repository API
    """

    # Whilst these are enforced by PostgreSQL in the Splitgraph schema, it's good to
    # prevalidate namespace/repository values when the Repository object is constructed.
    _MAX_NAMESPACE_LEN = 64
    _MAX_REPOSITORY_LEN = 64
    _NAMESPACE_RE = re.compile(r"^[-A-Za-z0-9_]*$")
    _REPOSITORY_RE = re.compile(r"^[-A-Za-z0-9_]+$")

    def __init__(
        self,
        namespace: str,
        repository: str,
        engine: Optional[PostgresEngine] = None,
        object_engine: Optional[PostgresEngine] = None,
        object_manager: Optional[ObjectManager] = None,
    ) -> None:
        if len(namespace) > self._MAX_NAMESPACE_LEN or not self._NAMESPACE_RE.match(namespace):
            raise ValueError(
                f"Invalid namespace '{namespace}'. Namespace must contain at most 64 "
                "alphanumerics, dashes or underscores."
            )

        if len(repository) > self._MAX_REPOSITORY_LEN or not self._REPOSITORY_RE.match(repository):
            raise ValueError(
                f"Invalid repository '{repository}'. Repository must contain at most 64 "
                "alphanumerics, dashes or underscores."
            )

        self.namespace = namespace
        self.repository = repository

        self.engine = engine or get_engine()
        # Add an option to use a different engine class for storing cached table fragments (e.g. a different
        # PostgreSQL connection or even a different database engine altogether).
        self.object_engine = object_engine or self.engine
        self.images = ImageManager(self)

        # consider making this an injected/a singleton for a given engine
        # since it's global for the whole engine but calls (e.g. repo.objects.cleanup()) make it
        # look like it's the manager for objects related to a repo.
        self.objects = object_manager or ObjectManager(
            object_engine=self.object_engine, metadata_engine=self.engine
        )

    @classmethod
    def from_template(
        cls,
        template: "Repository",
        namespace: Optional[str] = None,
        repository: Optional[str] = None,
        engine: Optional[PostgresEngine] = None,
        object_engine: Optional[PostgresEngine] = None,
    ) -> "Repository":
        """Create a Repository from an existing one replacing some of its attributes."""
        # If engine has been overridden but not object_engine, also override the object_engine (maintain
        # the intended behaviour of overriding engine repointing the whole repository)
        return cls(
            namespace or template.namespace,
            repository or template.repository,
            engine or template.engine,
            object_engine or engine or template.object_engine,
        )

    @classmethod
    def from_schema(cls, schema: str) -> "Repository":
        """Convert a Postgres schema name of the format `namespace/repository` to a Splitgraph repository object."""
        if "/" in schema:
            namespace, repository = schema.split("/")
            return cls(namespace, repository)
        return cls("", schema)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Repository):
            return NotImplemented
        return self.namespace == other.namespace and self.repository == other.repository

    def to_schema(self) -> str:
        """Returns the engine schema that this repository gets checked out into."""
        return self.namespace + "/" + self.repository if self.namespace else self.repository

    def __repr__(self) -> str:
        repr = "Repository %s on %s" % (self.to_schema(), self.engine.name)
        if self.engine != self.object_engine:
            repr += " (object engine %s)" % self.object_engine.name
        return repr

    __str__ = to_schema

    def __hash__(self) -> int:
        return hash(self.namespace) * hash(self.repository)

    # --- GENERAL REPOSITORY MANAGEMENT ---

    def rollback_engines(self) -> None:
        """
        Rollback the underlying transactions on both engines that the repository uses.
        """
        self.engine.rollback()
        if self.engine != self.object_engine:
            self.object_engine.rollback()

    def commit_engines(self) -> None:
        """
        Commit the underlying transactions on both engines that the repository uses.
        """
        self.engine.commit()
        if self.engine != self.object_engine:
            self.object_engine.commit()

    @manage_audit
    def init(self) -> None:
        """
        Initializes an empty repo with an initial commit (hash 0000...)
        """
        self.object_engine.create_schema(self.to_schema())
        initial_image = "0" * 64
        self.engine.run_sql(
            insert("images", ("image_hash", "namespace", "repository", "parent_id", "created")),
            (initial_image, self.namespace, self.repository, None, datetime.utcnow()),
        )
        # Strictly speaking this is redundant since the checkout (of the "HEAD" commit) updates the tag table.
        self.engine.run_sql(
            insert("tags", ("namespace", "repository", "image_hash", "tag")),
            (self.namespace, self.repository, initial_image, "HEAD"),
        )

    def delete(self, unregister: bool = True, uncheckout: bool = True) -> None:
        """
        Discards all changes to a given repository and optionally all of its history,
        as well as deleting the Postgres schema that it might be checked out into.
        Doesn't delete any cached physical objects.

        After performing this operation, this object becomes invalid and must be discarded,
        unless init() is called again.

        :param unregister: Whether to purge repository history/metadata
        :param uncheckout: Whether to delete the actual checked out repo. This has no effect
            if the repository is backed by a registry (rather than a local engine).
        """
        # Make sure to discard changes to this repository if they exist, otherwise they might
        # be applied/recorded if a new repository with the same name appears.
        if uncheckout and not self.object_engine.registry:
            self.object_engine.discard_pending_changes(self.to_schema())

            # Dispose of the foreign servers (LQ FDW, other FDWs) for this schema if it exists
            # (otherwise its connection won't be recycled and we can get deadlocked).
            self.object_engine.run_sql(
                SQL("DROP SERVER IF EXISTS {} CASCADE").format(
                    Identifier("%s_lq_checkout_server" % self.to_schema())
                )
            )
            self.object_engine.run_sql(
                SQL("DROP SERVER IF EXISTS {} CASCADE").format(
                    Identifier(self.to_schema() + "_server")
                )
            )
            self.object_engine.run_sql(
                SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(self.to_schema()))
            )
        if unregister:
            # Use the API call in case we're deleting a remote repository.
            self.engine.run_sql(
                SQL("SELECT {}.delete_repository(%s,%s)").format(Identifier(SPLITGRAPH_API_SCHEMA)),
                (self.namespace, self.repository),
            )

            # On local repos, also forget about the repository's upstream.
            if not self.object_engine.registry:
                self.object_engine.run_sql(
                    SQL("DELETE FROM {}.{} WHERE namespace = %s AND repository = %s").format(
                        Identifier(SPLITGRAPH_META_SCHEMA), Identifier("upstream")
                    ),
                    (self.namespace, self.repository),
                )
        self.engine.commit()

    @property
    def upstream(self):
        """
        The remote upstream repository that this local repository tracks.
        """
        result = self.engine.run_sql(
            select(
                "upstream",
                "remote_name, remote_namespace, remote_repository",
                "namespace = %s AND repository = %s",
            ),
            (self.namespace, self.repository),
            return_shape=ResultShape.ONE_MANY,
        )
        if result is None:
            return result
        try:
            engine = get_engine(result[0])
        except KeyError:
            logging.warning(
                "Repository %s/%s has upstream on remote %s which doesn't exist in the config.",
                self.namespace,
                self.repository,
                result[0],
            )
            return None

        return Repository(namespace=result[1], repository=result[2], engine=engine)

    @upstream.setter
    def upstream(self, remote_repository: "Repository"):
        """
        Sets the upstream remote + repository that this repository tracks.

        :param remote_repository: Remote Repository object
        """
        self.engine.run_sql(
            SQL(
                "INSERT INTO {0}.upstream (namespace, repository, "
                "remote_name, remote_namespace, remote_repository) VALUES (%s, %s, %s, %s, %s)"
                " ON CONFLICT (namespace, repository) DO UPDATE SET "
                "remote_name = excluded.remote_name, remote_namespace = excluded.remote_namespace, "
                "remote_repository = excluded.remote_repository WHERE "
                "upstream.namespace = excluded.namespace "
                "AND upstream.repository = excluded.repository"
            ).format(Identifier(SPLITGRAPH_META_SCHEMA)),
            (
                self.namespace,
                self.repository,
                remote_repository.engine.name,
                remote_repository.namespace,
                remote_repository.repository,
            ),
        )

    @upstream.deleter
    def upstream(self):
        """
        Deletes the upstream remote + repository for a local repository.
        """
        self.engine.run_sql(
            SQL("DELETE FROM {0}.upstream WHERE namespace = %s AND repository = %s").format(
                Identifier(SPLITGRAPH_META_SCHEMA)
            ),
            (self.namespace, self.repository),
            return_shape=None,
        )

    def get_size(self) -> int:
        """
        Get the physical size used by the repository's data, counting objects that are used
        by multiple images only once. This is calculated from the metadata, the on-disk
        footprint might be smaller if not all of repository's objects have been downloaded.

        :return: Size of the repository in bytes.
        """
        return cast(
            int,
            self.engine.run_sql(
                select("get_repository_size", table_args="(%s,%s)", schema=SPLITGRAPH_API_SCHEMA),
                (self.namespace, self.repository),
                return_shape=ResultShape.ONE_ONE,
            )
            or 0,
        )

    def get_local_size(self) -> int:
        """
        Get the actual size used by this repository's downloaded objects.

        This might still be double-counted if the repository shares objects
        with other repositores.

        :return: Size of the repository in bytes.
        """
        repo_objects = self.objects.get_objects_for_repository(self)
        local_objects = self.objects.get_downloaded_objects(limit_to=repo_objects)
        local_object_meta = self.objects.get_object_meta(local_objects)
        return sum(o.size for o in local_object_meta.values())

    # --- COMMITS / CHECKOUTS ---

    @contextmanager
    def materialized_table(
        self, table_name: str, image_hash: Optional[str]
    ) -> Iterator[Tuple[str, str]]:
        """A context manager that returns a pointer to a read-only materialized table in a given image.
        The table is deleted on exit from the context manager.

        :param table_name: Name of the table
        :param image_hash: Image hash to materialize
        :return: (schema, table_name) where the materialized table is located.
        """
        if image_hash is None:
            # No image hash -- just return the current staging table.
            yield self.to_schema(), table_name
            return  # make sure we don't fall through after the user is finished

        table = self.images.by_hash(image_hash).get_table(table_name)
        # Materialize the table even if it's a single object to discard the upsert-delete flag.
        new_id = get_temporary_table_id()
        table.materialize(new_id, destination_schema=SPLITGRAPH_META_SCHEMA)
        try:
            yield SPLITGRAPH_META_SCHEMA, new_id
        finally:
            # Maybe some cache management/expiry strategies here
            self.object_engine.delete_table(SPLITGRAPH_META_SCHEMA, new_id)
            self.object_engine.commit()

    @property
    def head_strict(self) -> Image:
        """Return the HEAD image for the repository. Raise an exception if the repository
         isn't checked out."""
        return cast(Image, self.images.by_tag("HEAD", raise_on_none=True))

    @property
    def head(self) -> Optional[Image]:
        """Return the HEAD image for the repository or None if the repository isn't checked out."""
        return self.images.by_tag("HEAD", raise_on_none=False)

    @manage_audit
    def uncheckout(self, force: bool = False) -> None:
        """
        Deletes the schema that the repository is checked out into

        :param force: Discards all pending changes to the schema.
        """
        if not self.head:
            return
        if self.has_pending_changes():
            if not force:
                raise CheckoutError(
                    "{0} has pending changes! Pass force=True or do sgr checkout -f {0}:HEAD".format(
                        self.to_schema()
                    )
                )
            logging.warning("%s has pending changes, discarding...", self.to_schema())

        # Delete the schema and remove the HEAD tag
        self.delete(unregister=False, uncheckout=True)
        self.head.delete_tag("HEAD")

    def commit(
        self,
        image_hash: Optional[str] = None,
        comment: Optional[str] = None,
        snap_only: bool = False,
        chunk_size: Optional[int] = None,
        split_changeset: bool = False,
        extra_indexes: Optional[Dict[str, ExtraIndexInfo]] = None,
        in_fragment_order: Optional[Dict[str, List[str]]] = None,
        overwrite: bool = False,
    ) -> Image:
        """
        Commits all pending changes to a given repository, creating a new image.

        :param image_hash: Hash of the commit. Chosen by random if unspecified.
        :param comment: Optional comment to add to the commit.
        :param snap_only: If True, will store the table as a full snapshot instead of delta compression
        :param chunk_size: For tables that are stored as snapshots (new tables and where `snap_only` has been passed,
            the table will be split into fragments of this many rows.
        :param split_changeset: If True, splits the changeset into multiple fragments based on
            the PK regions spanned by the current table fragments. For example, if the original table
            consists of 2 fragments, first spanning rows 1-10000, second spanning rows 10001-20000 and the
            change alters rows 1, 10001 and inserts a row with PK 20001, this will record the change as
            3 fragments: one inheriting from the first original fragment, one inheriting from the second
            and a brand new fragment. This increases the number of fragments in total but means that fewer rows
            will need to be scanned to satisfy a query.
            If False, the changeset will be stored as a single fragment inheriting from the last fragment in the
            table.
        :param extra_indexes: Dictionary of {table: index_type: column: index_specific_kwargs}.
        :param in_fragment_order: Dictionary of {table: list of columns}. If specified, will
        sort the data inside each chunk by this/these key(s) for each table.
        :param overwrite: If an object already exists, will force recreate it.

        :return: The newly created Image object.
        """

        logging.info("Committing %s...", self.to_schema())

        self.object_engine.commit()

        # HEAD can be None (if this is the first commit in this repository)
        head = self.head
        if image_hash is None:
            # Generate a random hexadecimal hash for new images
            image_hash = "{:064x}".format(getrandbits(256))

        self.images.add(head.image_hash if head else None, image_hash, comment=comment)
        self._commit(
            head,
            image_hash,
            snap_only=snap_only,
            chunk_size=chunk_size,
            split_changeset=split_changeset,
            extra_indexes=extra_indexes,
            in_fragment_order=in_fragment_order,
            overwrite=overwrite,
        )

        set_head(self, image_hash)
        manage_audit_triggers(self.engine, self.object_engine)
        self.commit_engines()
        return self.images.by_hash(image_hash)

    def _commit(
        self,
        head: Optional[Image],
        image_hash: str,
        snap_only: bool = False,
        chunk_size: Optional[int] = None,
        split_changeset: bool = False,
        schema: str = None,
        extra_indexes: Optional[Dict[str, ExtraIndexInfo]] = None,
        in_fragment_order: Optional[Dict[str, List[str]]] = None,
        overwrite: bool = False,
    ) -> None:
        """
        Reads the recorded pending changes to all tables in a given checked-out image,
        conflates them and possibly stores them as new object(s) as follows:

            * If a table has been created or there has been a schema change, it's only stored as a full snapshot.
            * If a table hasn't changed since the last revision, no new objects are created and it's linked to the
                previous objects belonging to the last revision.
            * Otherwise, the table is stored as a conflated (1 change per PK) patch.
        """
        schema = schema or self.to_schema()
        extra_indexes: Dict[str, ExtraIndexInfo] = extra_indexes or {}
        in_fragment_order: Dict[str, List[str]] = in_fragment_order or {}
        chunk_size = chunk_size or int(get_singleton(CONFIG, "SG_COMMIT_CHUNK_SIZE"))

        changed_tables = self.object_engine.get_changed_tables(schema)
        tracked_tables = self.object_engine.get_tracked_tables()

        for table in self.object_engine.get_all_tables(schema):
            if self.object_engine.get_table_type(schema, table) == "VIEW":
                logging.warning(
                    "Table %s.%s is a view. Splitgraph currently doesn't "
                    "support views and this table will not be in the image.",
                    schema,
                    table,
                )
                continue

            try:
                table_info: Optional[Table] = head.get_table(table) if head else None
            except TableNotFoundError:
                table_info = None

            # Store as a full copy if this is a new table, there's been a schema change or we were told to.
            # Also store the full copy if the audit trigger on the table is missing: this indicates
            # that the table was dropped and recreated.
            new_schema = self.object_engine.get_full_table_schema(schema, table)
            if (
                not table_info
                or snap_only
                or not _schema_compatible(table_info.table_schema, new_schema)
                or (schema, table) not in tracked_tables
            ):
                self.objects.record_table_as_base(
                    self,
                    table,
                    image_hash,
                    chunk_size=chunk_size,
                    source_schema=schema,
                    extra_indexes=extra_indexes.get(table),
                    in_fragment_order=in_fragment_order.get(table),
                    overwrite=overwrite,
                )
                continue

            # If the table has changed, look at the audit log and store it as a delta.
            if table in changed_tables:
                self.objects.record_table_as_patch(
                    table_info,
                    schema,
                    image_hash,
                    new_schema_spec=new_schema,
                    split_changeset=split_changeset,
                    extra_indexes=extra_indexes.get(table),
                    in_fragment_order=in_fragment_order.get(table),
                    overwrite=overwrite,
                )
                continue

            # If the table wasn't changed, point the image to the old table
            self.objects.register_tables(
                self, [(image_hash, table, new_schema, table_info.objects)]
            )

        # Make sure that all pending changes have been discarded by this point (e.g. if we created just a snapshot for
        # some tables and didn't consume the audit log).
        # NB if we allow partial commits, this will have to be changed (only discard for committed tables).
        self.object_engine.discard_pending_changes(schema)

    def has_pending_changes(self) -> bool:
        """
        Detects if the repository has any pending changes (schema changes, table additions/deletions, content changes).
        """
        head = self.head
        if not head:
            # If the repo isn't checked out, no point checking for changes.
            return False
        for table in self.object_engine.get_all_tables(self.to_schema()):
            diff = self.diff(table, head.image_hash, None, aggregate=True)
            if diff != (0, 0, 0) and diff is not None:
                return True
        return False

    # --- TAG AND IMAGE MANAGEMENT ---

    def get_all_hashes_tags(self) -> List[Tuple[Optional[str], str]]:
        """
        Gets all tagged images and their hashes in a given repository.

        :return: List of (image_hash, tag)
        """
        return cast(
            List[Tuple[Optional[str], str]],
            self.engine.run_sql(
                select(
                    "get_tagged_images",
                    "image_hash, tag",
                    schema=SPLITGRAPH_API_SCHEMA,
                    table_args="(%s,%s)",
                ),
                (self.namespace, self.repository),
            ),
        )

    def set_tags(self, tags: Dict[str, Optional[str]]) -> None:
        """
        Sets tags for multiple images.

        :param tags: List of (image_hash, tag)
        """
        args = []
        for tag, image_id in tags.items():
            if tag != "HEAD":
                assert image_id is not None
                args.append((image_id, tag))
        set_tags_batch(self, args)

    def run_sql(
        self,
        sql: Union[Composed, str],
        arguments: Optional[Any] = None,
        return_shape: ResultShape = ResultShape.MANY_MANY,
    ) -> Any:
        """Execute an arbitrary SQL statement inside of this repository's checked out schema."""
        return self.object_engine.run_sql_in(self.to_schema(), sql, arguments, return_shape)

    def dump(self, stream: TextIOWrapper, exclude_object_contents: bool = False) -> None:
        """
        Creates an SQL dump with the metadata required for the repository and all of its objects.

        :param stream: Stream to dump the data into.
        :param exclude_object_contents: Only dump the metadata but not the actual object contents.
        """
        # First, go through the metadata tables required to reconstruct the repository.
        stream.write("--\n-- Images --\n--\n")
        self.engine.dump_table_sql(
            SPLITGRAPH_META_SCHEMA,
            "images",
            stream,
            where="namespace = %s AND repository = %s",
            where_args=(self.namespace, self.repository),
        )

        # Add objects (need to come before tables: we check that objects for inserted tables are registered.
        required_objects: Set[str] = set()
        for image in self.images:
            for table_name in image.get_tables():
                required_objects.update(image.get_table(table_name).objects)

        object_qual = (
            "object_id IN (" + ",".join(itertools.repeat("%s", len(required_objects))) + ")"
        )

        stream.write("\n--\n-- Objects --\n--\n")
        # To avoid conflicts, we just delete the object records if they already exist
        with self.engine.connection.cursor() as cur:
            for table_name in ("objects", "object_locations"):
                stream.write(
                    cur.mogrify(
                        SQL("DELETE FROM {}.{} WHERE ").format(
                            Identifier(SPLITGRAPH_META_SCHEMA), Identifier(table_name)
                        )
                        + SQL(object_qual),
                        list(required_objects),
                    ).decode("utf-8")
                )
                stream.write(";\n\n")
                self.engine.dump_table_sql(
                    SPLITGRAPH_META_SCHEMA,
                    table_name,
                    stream,
                    where=object_qual,
                    where_args=list(required_objects),
                )

        stream.write("\n--\n-- Tables --\n--\n")
        self.engine.dump_table_sql(
            SPLITGRAPH_META_SCHEMA,
            "tables",
            stream,
            where="namespace = %s AND repository = %s",
            where_args=(self.namespace, self.repository),
        )

        stream.write("\n--\n-- Tags --\n--\n")
        self.engine.dump_table_sql(
            SPLITGRAPH_META_SCHEMA,
            "tags",
            stream,
            where="namespace = %s AND repository = %s AND tag != 'HEAD'",
            where_args=(self.namespace, self.repository),
        )

        if not exclude_object_contents:
            with self.engine.connection.cursor() as cur:
                stream.write("\n--\n-- Object contents --\n--\n")

                # Finally, dump the actual objects
                for object_id in required_objects:
                    stream.write(
                        cur.mogrify(
                            SQL("DROP FOREIGN TABLE IF EXISTS {}.{};\n").format(
                                Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
                            )
                        ).decode("utf-8")
                    )
                    self.object_engine.dump_object(object_id, stream, schema=SPLITGRAPH_META_SCHEMA)
                    stream.write("\n")

    # --- IMPORTING TABLES ---

    @manage_audit
    def import_tables(
        self,
        tables: Sequence[str],
        source_repository: "Repository",
        source_tables: Sequence[str],
        image_hash: Optional[str] = None,
        foreign_tables: bool = False,
        do_checkout: bool = True,
        target_hash: Optional[str] = None,
        table_queries: Optional[Sequence[bool]] = None,
        parent_hash: Optional[str] = None,
        wrapper: Optional[str] = FDW_CLASS,
        skip_validation: bool = False,
    ) -> str:
        """
        Creates a new commit in target_repository with one or more tables linked to already-existing tables.
        After this operation, the HEAD of the target repository moves to the new commit and the new tables are
        materialized.

        :param tables: If not empty, must be the list of the same length as `source_tables` specifying names to store
            them under in the target repository.
        :param source_repository: Repository to import tables from.
        :param source_tables: List of tables to import. If empty, imports all tables.
        :param image_hash: Image hash in the source repository to import tables from.
            Uses the current source HEAD by default.
        :param foreign_tables: If True, copies all source tables to create a series of new snapshots instead of
            treating them as Splitgraph-versioned tables. This is useful for adding brand new tables
            (for example, from an FDW-mounted table).
        :param do_checkout: If False, doesn't check out the newly created image.
        :param target_hash: Hash of the new image that tables is recorded under. If None, gets chosen at random.
        :param table_queries: If not [], it's treated as a Boolean mask showing which entries in the `tables` list are
            instead SELECT SQL queries that form the target table. The queries have to be non-schema qualified and work
            only against tables in the source repository. Each target table created is the result of the respective SQL
            query. This is committed as a new snapshot.
        :param parent_hash: If not None, must be the hash of the image to base the new image on.
            Existing tables from the parent image are preserved in the new image. If None, the current repository
            HEAD is used.
        :param wrapper: Override the default class for the layered querying foreign data wrapper.
        :param skip_validation: Don't validate SQL used in import statements (used by the Splitfile executor that pre-formats the SQL).
        :return: Hash that the new image was stored under.
        """
        # Sanitize/validate the parameters and call the internal function.
        if table_queries is None:
            table_queries = []
        target_hash = target_hash or "{:064x}".format(getrandbits(256))

        image: Optional[Image]
        if not foreign_tables:
            image = (
                source_repository.images.by_hash(image_hash)
                if image_hash
                else source_repository.head_strict
            )
        else:
            image = None

        if not source_tables:
            if foreign_tables:
                source_tables = source_repository.object_engine.get_all_tables(
                    source_repository.to_schema()
                )
            else:
                assert image is not None
                source_tables = image.get_tables()
        if not tables:
            if table_queries:
                raise ValueError("target_tables has to be defined if table_queries is True!")
            tables = source_tables
        if not table_queries:
            table_queries = [False] * len(tables)
        if len(tables) != len(source_tables) or len(source_tables) != len(table_queries):
            raise ValueError("tables, source_tables and table_queries have mismatching lengths!")

        if parent_hash:
            existing_tables = self.images[parent_hash].get_tables()
        else:
            try:
                parent_hash = self.head.image_hash if self.head else None
            except RepositoryNotFoundError:
                parent_hash = None
            existing_tables = self.object_engine.get_all_tables(self.to_schema())
        clashing = [t for t in tables if t in existing_tables]
        if clashing:
            raise ValueError("Table(s) %r already exist(s) at %s!" % (clashing, self))

        return self._import_tables(
            image,
            tables,
            source_repository,
            target_hash,
            source_tables,
            do_checkout,
            table_queries,
            foreign_tables,
            parent_hash,
            wrapper,
            skip_validation,
        )

    def _import_tables(
        self,
        image: Optional[Image],
        tables: Sequence[str],
        source_repository: "Repository",
        target_hash: str,
        source_tables: Sequence[str],
        do_checkout: bool,
        table_queries: Sequence[bool],
        foreign_tables: bool,
        base_hash: Optional[str],
        wrapper: Optional[str],
        skip_validation: bool,
    ) -> str:
        # This importing route only supported between local repos.
        assert self.engine == source_repository.engine
        assert self.object_engine == source_repository.object_engine
        if do_checkout:
            self.object_engine.create_schema(self.to_schema())

        self.images.add(
            base_hash,
            target_hash,
            comment="Importing %s from %s" % (pluralise("table", len(tables)), source_repository),
        )

        # Materialize the actual tables in the target repository and register them.
        for source_table, target_table, is_query in zip(source_tables, tables, table_queries):
            # For foreign tables/SELECT queries, we define a new object/table instead.
            if is_query and not foreign_tables:
                # If we're importing a query from another Splitgraph image, we can use LQ to satisfy it.
                # This could get executed for the whole import batch as opposed to for every import query
                # but the overhead of setting up an LQ schema is fairly small.
                assert image is not None
                with image.query_schema(wrapper=wrapper) as tmp_schema:
                    self._import_new_table(
                        tmp_schema,
                        source_table,
                        target_hash,
                        target_table,
                        is_query,
                        do_checkout,
                        skip_validation,
                    )
            elif foreign_tables:
                self._import_new_table(
                    source_repository.to_schema(),
                    source_table,
                    target_hash,
                    target_table,
                    is_query,
                    do_checkout,
                )
            else:
                assert image is not None
                table_obj = image.get_table(source_table)
                self.objects.register_tables(
                    self, [(target_hash, target_table, table_obj.table_schema, table_obj.objects)]
                )
                if do_checkout:
                    table_obj.materialize(target_table, destination_schema=self.to_schema())
        # Register the existing tables at the new commit as well.
        if base_hash is not None:
            # Maybe push this into the tables API (currently have to make 2 queries)
            self.engine.run_sql(
                SQL(
                    """INSERT INTO {0}.tables (namespace, repository, image_hash,
                    table_name, table_schema, object_ids) (SELECT %s, %s, %s, table_name, table_schema, object_ids
                    FROM {0}.tables WHERE namespace = %s AND repository = %s AND image_hash = %s)"""
                ).format(Identifier(SPLITGRAPH_META_SCHEMA)),
                (
                    self.namespace,
                    self.repository,
                    target_hash,
                    self.namespace,
                    self.repository,
                    base_hash,
                ),
            )
        if do_checkout:
            set_head(self, target_hash)
        return target_hash

    def _import_new_table(
        self,
        source_schema: str,
        source_table: str,
        target_hash: str,
        target_table: str,
        is_query: bool,
        do_checkout: bool,
        skip_validation: bool = False,
    ) -> List[str]:
        # First, import the query (or the foreign table) into a temporary table.
        tmp_object_id = get_temporary_table_id()
        if is_query:
            # is_query precedes foreign_tables: if we're importing using a query, we don't care if it's a
            # foreign table or not since we're storing it as a full snapshot.
            if not skip_validation:
                source_table = validate_import_sql(source_table)

            # Prepend query planner instructions to the query
            self.object_engine.run_sql_in(
                source_schema,
                SQL(get_singleton(CONFIG, "SG_LQ_TUNING"))
                + SQL("CREATE TABLE {}.{} AS ").format(
                    Identifier(SPLITGRAPH_META_SCHEMA), Identifier(tmp_object_id)
                )
                + SQL(source_table),
            )
        else:
            self.object_engine.copy_table(
                source_schema, source_table, SPLITGRAPH_META_SCHEMA, tmp_object_id
            )

        # This is kind of a waste: if the table is indeed new (and fits in one chunk), the fragment manager will copy it
        # over once again and give it the new object ID. Maybe the fragment manager could rename the table in this case.
        actual_objects = self.objects.record_table_as_base(
            self,
            target_table,
            target_hash,
            source_schema=SPLITGRAPH_META_SCHEMA,
            source_table=tmp_object_id,
        )
        self.object_engine.delete_table(SPLITGRAPH_META_SCHEMA, tmp_object_id)
        if do_checkout:
            self.images.by_hash(target_hash).get_table(target_table).materialize(
                target_table, self.to_schema()
            )
        return actual_objects

    # --- SYNCING WITH OTHER REPOSITORIES ---

    def push(
        self,
        remote_repository: Optional["Repository"] = None,
        overwrite_objects: bool = False,
        reupload_objects: bool = False,
        overwrite_tags: bool = False,
        handler: str = "DB",
        handler_options: Optional[Dict[str, Any]] = None,
        single_image: Optional[str] = None,
    ) -> "Repository":
        """
        Inverse of ``pull``: Pushes all local changes to the remote and uploads new objects.

        :param remote_repository: Remote repository to push changes to. If not specified, the current
            upstream is used.
        :param handler: Name of the handler to use to upload objects. Use `DB` to push them to the remote or `S3`
            to store them in an S3 bucket.
        :param overwrite_objects: If True, will overwrite object metadata on the remote repository for existing objects.
        :param reupload_objects: If True, will reupload objects for which metadata is uploaded.
        :param overwrite_tags: If True, will overwrite existing tags on the remote repository.
        :param handler_options: Extra options to pass to the handler. For example, see
            :class:`splitgraph.hooks.s3.S3ExternalObjectHandler`.
        :param single_image: Limit the upload to a single image hash/tag.
        """
        remote_repository = remote_repository or self.upstream
        if not remote_repository:
            raise ValueError(
                "No remote repository specified and no upstream found for %s!" % self.to_schema()
            )

        try:
            _sync(
                target=remote_repository,
                source=self,
                download=False,
                overwrite_objects=overwrite_objects,
                reupload_objects=reupload_objects,
                overwrite_tags=overwrite_tags,
                handler=handler,
                handler_options=handler_options,
                single_image=single_image,
            )

            if not self.upstream:
                self.upstream = remote_repository
                logging.info("Setting upstream for %s to %s.", self, remote_repository)
        finally:
            # Don't commit the connection here: _sync is supposed to do it itself
            # after a successful push/pull.
            remote_repository.engine.close()
        return remote_repository

    def pull(
        self,
        download_all: Optional[bool] = False,
        overwrite_objects: bool = False,
        overwrite_tags: bool = False,
        single_image: Optional[str] = None,
    ) -> None:
        """
        Synchronizes the state of the local Splitgraph repository with its upstream, optionally downloading all new
        objects created on the remote.

        :param download_all: If True, downloads all objects and stores them locally. Otherwise, will only download
            required objects when a table is checked out.
        :param overwrite_objects: If True, will overwrite object metadata on the local repository for existing objects.
        :param overwrite_tags: If True, will overwrite existing tags.
        :param single_image: Limit the download to a single image hash/tag.
        """
        if not self.upstream:
            raise ValueError("No upstream found for repository %s!" % self.to_schema())

        clone(
            remote_repository=self.upstream,
            local_repository=self,
            download_all=download_all,
            overwrite_objects=overwrite_objects,
            overwrite_tags=overwrite_tags,
            single_image=single_image,
        )

    def diff(
        self,
        table_name: str,
        image_1: Union[Image, str],
        image_2: Optional[Union[Image, str]],
        aggregate: bool = False,
    ) -> Union[bool, Tuple[int, int, int], List[Tuple[bool, Tuple]], None]:
        """
        Compares the state of a table in different images by materializing both tables into a temporary space
        and comparing them row-to-row.

        :param table_name: Name of the table.
        :param image_1: First image hash / object. If None, uses the state of the current staging area.
        :param image_2: Second image hash / object. If None, uses the state of the current staging area.
        :param aggregate: If True, returns a tuple of integers denoting added, removed and updated rows between
            the two images.
        :return: If the table doesn't exist in one of the images, returns True if it was added and False if it was
            removed. If `aggregate` is True, returns the aggregation of changes as specified before.
            Otherwise, returns a list of changes where each change is a tuple of
            `(True for added, False for removed, row contents)`.
        """

        if isinstance(image_1, str):
            image_1 = self.images.by_hash(image_1)
        if isinstance(image_2, str):
            image_2 = self.images.by_hash(image_2)

        # If the table is of an unsupported type (e.g. a view), ignore it
        if (
            self.object_engine.table_exists(self.to_schema(), table_name)
            and self.object_engine.get_table_type(self.to_schema(), table_name) == "VIEW"
        ):
            return None

        # If the table doesn't exist in the first or the second image, short-circuit and
        # return the bool.
        if not table_exists_at(self, table_name, image_1):
            return True
        if not table_exists_at(self, table_name, image_2):
            return False

        # Special case: if diffing HEAD and staging (with aggregation), we can return that directly.
        if (
            image_1 == self.head
            and image_2 is None
            and aggregate
            and (self.to_schema(), table_name) in self.object_engine.get_tracked_tables()
        ):
            return aggregate_changes(
                cast(
                    List[Tuple[int, int]],
                    self.object_engine.get_pending_changes(
                        self.to_schema(), table_name, aggregate=True
                    ),
                )
            )

        # If the table is the same in the two images, short circuit as well.
        if image_2 is not None:
            if set(image_1.get_table(table_name).objects) == set(
                image_2.get_table(table_name).objects
            ):
                return [] if not aggregate else (0, 0, 0)

        # Materialize both tables and compare them side-by-side.
        # TODO we can aggregate chunks in a similar way that LQ does it.
        return slow_diff(self, table_name, _hash(image_1), _hash(image_2), aggregate)


def import_table_from_remote(
    remote_repository: "Repository",
    remote_tables: List[str],
    remote_image_hash: str,
    target_repository: "Repository",
    target_tables: List[Any],
    target_hash: str = None,
) -> None:
    """
    Shorthand for importing one or more tables from a yet-uncloned remote. Here, the remote image hash is required,
    as otherwise we aren't necessarily able to determine what the remote head is.

    :param remote_repository: Remote Repository object
    :param remote_tables: List of remote tables to import
    :param remote_image_hash: Image hash to import the tables from
    :param target_repository: Target repository to import the tables to
    :param target_tables: Target table aliases
    :param target_hash: Hash of the image that's created with the import. Default random.
    """

    # In the future, we could do some vaguely intelligent interrogation of the remote to directly copy the required
    # metadata (object locations and relationships) into the local mountpoint. However, since the metadata is fairly
    # lightweight (we never download unneeded objects), we just clone it into a temporary mountpoint,
    # do the import into the target and destroy the temporary mp.
    tmp_mountpoint = Repository(
        namespace=remote_repository.namespace,
        repository=remote_repository.repository + "_clone_tmp",
    )

    clone(remote_repository, local_repository=tmp_mountpoint, download_all=False)
    target_repository.import_tables(
        target_tables,
        tmp_mountpoint,
        remote_tables,
        image_hash=remote_image_hash,
        target_hash=target_hash,
    )

    tmp_mountpoint.delete()
    target_repository.commit_engines()


def table_exists_at(
    repository: "Repository", table_name: str, image: Optional[Image] = None
) -> bool:
    """Determines whether a given table exists in a Splitgraph image without checking it out. If `image_hash` is None,
    determines whether the table exists in the current staging area."""
    if image is None:
        return repository.object_engine.table_exists(repository.to_schema(), table_name)
    else:
        try:
            image.get_table(table_name)
            return True
        except TableNotFoundError:
            return False


def _sync(
    target: "Repository",
    source: "Repository",
    download: bool = True,
    overwrite_objects: bool = False,
    reupload_objects: bool = False,
    overwrite_tags: bool = False,
    handler: str = "DB",
    handler_options: Optional[Dict[str, Any]] = None,
    single_image: Optional[str] = None,
) -> None:
    """
    Generic routine for syncing two repositories: fetches images, hashes, objects and tags
    on `source` that don't exist in `target`.

    Common code between push and pull, since the only difference between those routines is that
    uploading and downloading objects are different operations.

    :param target: Target Repository object
    :param source: Source Repository object
    :param download: If True, uses the download routines to download physical objects to self.
        If False, uses the upload routines to get `source` to upload physical objects to self / external.
    :param overwrite_objects: If True, overwrites remote object metadata for the target repository.
    :param reupload_objects: If True, will reupload objects for which metadata is uploaded.
    :param overwrite_tags: If True, overwrites tags for all (if syncing the whole repository)
        or the single image.
    :param handler: Upload handler
    :param handler_options: Upload handler options
    :param single_image: Limit the download/upload to a single image hash/tag.
    """
    if handler_options is None:
        handler_options = {}

    # Get the remote log and the list of objects we need to fetch.
    logging.info("Gathering remote metadata...")

    partial_upload_failure: Optional[IncompleteObjectUploadError] = None

    # Run a read-only transaction that gets images/objects we need to download
    new_images, table_meta, object_locations, object_meta, tags = gather_sync_metadata(
        target,
        source,
        overwrite_objects=overwrite_objects,
        single_image=single_image,
        overwrite_tags=overwrite_tags,
    )
    if not new_images and not object_meta and not object_locations and not tags:
        logging.info("No image/object metadata to pull.")
        return

    try:
        if download:
            target.objects.register_objects(list(object_meta.values()))
            target.objects.register_object_locations(object_locations)

            # Don't check anything out, keep the repo bare.
            set_head(target, None)
        else:
            # If we're overwriting, upload all objects that we're pushing metadata for
            # (apart from objects with a different namespace -- won't be able to overwrite those).
            new_objects = target.objects.get_new_objects(list(object_meta.keys()))

            objects_to_push = (
                new_objects
                if not reupload_objects
                else [o for o, om in object_meta.items() if om.namespace == target.namespace]
            )

            # Transaction handling here is finicky as we don't want to hold an open transaction
            # to the registry, blocking other clients. gather_sync_metadata was a self-contained
            # read-only transaction. The S3 uploader runs a self-contained RO transaction as well
            # to find out the URLs it's uploading objects to.

            try:
                new_uploads = source.objects.upload_objects(
                    target.objects,
                    objects_to_push,
                    handler=handler,
                    handler_params=handler_options,
                )
                new_locations = [(o, u, handler) for o, u in new_uploads if u]
                successful = [o for o, _ in new_uploads]
            except IncompleteObjectUploadError as e:
                partial_upload_failure = e
                new_locations = [
                    (o, u, handler)
                    for o, u in zip(e.successful_objects, e.successful_object_urls)
                    if u
                ]
                successful = e.successful_objects

            # Here we have to register the new objects after the upload but before we store
            # their external location (as access control for object_locations relies on the
            # object metadata being in place).
            # In addition, register all objects we had in object_meta apart from those that
            # we tried to upload and failed (if we're pushing overwriting object metadata,
            # object_meta might have more objects than what we actually intended to upload).

            # Note that this implicitly opens a short-lived write transaction to the target
            # (we register objects, their locations and then the actual tables and images
            # when we leave this if).
            target.objects.register_objects(
                [v for k, v in object_meta.items() if k not in objects_to_push or k in successful],
                namespace=target.namespace,
            )

            # Don't register locations for objects that we overwrote (actual URLs are supposed
            # to stay the same).
            new_locations = [o for o in new_locations if o[0] in new_objects]
            target.objects.register_object_locations(
                [o for o in set(object_locations + new_locations) if o[0] in successful]
            )
            source.objects.register_object_locations(new_locations)

            if partial_upload_failure:
                # If the upload has failed or was cancelled for some reason, still register
                # successful objects (give the caller a chance to continue the upload) but
                # not images or tables -- bail out now.
                target.commit_engines()
                source.commit_engines()
                if partial_upload_failure.reason:
                    raise partial_upload_failure.reason
                else:
                    raise partial_upload_failure

        # Register the new images / tables / tags.
        target.images.add_batch(new_images)
        target.objects.register_tables(target, table_meta)
        target.set_tags(tags)
    except Exception:
        logging.exception("Error during repository sync")
        target.rollback_engines()
        source.rollback_engines()
        raise

    target.commit_engines()
    source.commit_engines()

    logging.info(
        "%s metadata for %s, %s, %s and %s.",
        ("Fetched" if download else "Uploaded"),
        pluralise("image", len(new_images)),
        pluralise("table", len(table_meta)),
        pluralise("object", len(object_meta)),
        pluralise("tag", len([t for t in tags if t != "HEAD"])),
    )


def clone(
    remote_repository: Union["Repository", str],
    local_repository: Optional["Repository"] = None,
    overwrite_objects: bool = False,
    overwrite_tags: bool = False,
    download_all: Optional[bool] = False,
    single_image: Optional[str] = None,
) -> "Repository":
    """
    Clones a remote Splitgraph repository or synchronizes remote changes with the local ones.

    If the target repository has no set upstream engine, the source repository becomes its upstream.

    :param remote_repository: Remote Repository object to clone or the repository's name. If a name is passed,
        the repository will be looked up on the current lookup path in order to find the engine the repository
        belongs to.
    :param local_repository: Local repository to clone into. If None, uses the same name as the remote.
    :param download_all: If True, downloads all objects and stores them locally. Otherwise, will only download required
        objects when a table is checked out.
    :param overwrite_objects: If True, will overwrite object metadata on the local repository for existing objects.
    :param overwrite_tags: If True, will overwrite existing tags.
    :param single_image: If set, only get a single image with this hash/tag from the source.
    :return: A locally cloned Repository object.
    """
    if isinstance(remote_repository, str):
        remote_repository = lookup_repository(remote_repository, include_local=False)

    # Repository engine should be local by default
    if not local_repository:
        local_repository = Repository(remote_repository.namespace, remote_repository.repository)

    _sync(
        local_repository,
        remote_repository,
        download=True,
        overwrite_objects=overwrite_objects,
        overwrite_tags=overwrite_tags,
        single_image=single_image,
    )

    # Perform the optional download of all objects as a final step (normally we do it when the user
    # tries to check out a revision) and do it using the object manager as it has some handling
    # of error cases.
    if download_all:
        local_om = local_repository.objects
        with local_om.ensure_objects(
            table=None,
            objects=local_om.get_objects_for_repository(
                local_repository,
                image_hash=local_repository.images[single_image].image_hash
                if single_image
                else None,
            ),
            upstream_manager=remote_repository.objects,
        ):
            pass

    if not local_repository.upstream:
        local_repository.upstream = remote_repository

    return local_repository


def _hash(image: Optional[Image]) -> Optional[str]:
    return image.image_hash if image is not None else None


def _schema_compatible(old: TableSchema, new: TableSchema) -> bool:
    """
    Determines whether a table with a schema change can be delta compressed
    using data in the audit triggers.

    :param old: Old table schema
    :param new: New table schema
    :return: Boolean
    """

    # Currently we don't support schema changes at all but we do delta compress if
    # just the comments on some of the table's columns have changed.
    return [t[:4] for t in old] == [t[:4] for t in new]
