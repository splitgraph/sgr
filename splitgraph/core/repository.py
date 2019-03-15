"""
Public API for managing images in a Splitgraph repository.
"""

import itertools
import logging
from contextlib import contextmanager
from datetime import datetime
from random import getrandbits

from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.exceptions import SplitGraphException

from ._common import manage_audit_triggers, set_head, manage_audit, select, insert, ensure_metadata_schema, \
    aggregate_changes, slow_diff, prepare_publish_data, gather_sync_metadata
from .engine import repository_exists, lookup_repository, ResultShape, get_engine
from .image import Image, IMAGE_COLS
from .object_manager import ObjectManager, get_random_object_id
from .registry import publish_tag


class ImageManager:
    """Collects various image-related functions."""

    def __init__(self, repository):
        self.repository = repository
        self.engine = repository.engine

    def __call__(self):
        """Get all Image objects in the repository."""
        result = []
        for image in self.engine.run_sql(select("images", ','.join(IMAGE_COLS), "repository = %s AND namespace = %s") +
                                         SQL(" ORDER BY created"),
                                         (self.repository.repository, self.repository.namespace)):
            result.append(self._make_image(image))
        return result

    def _make_image(self, img_tuple):
        r_dict = {k: v for k, v in zip(IMAGE_COLS, img_tuple)}
        r_dict.update(repository=self.repository)
        return Image(**r_dict)

    def by_tag(self, tag, raise_on_none=True):
        """
        Returns an image with a given tag

        :param tag: Tag. 'latest' is a special case: it returns the most recent image in the repository.
        :param raise_on_none: Whether to raise an error or return None if the tag doesn't exist.
        """
        engine = self.engine
        if not repository_exists(self.repository) and raise_on_none:
            raise SplitGraphException("%s does not exist!" % str(self))

        if tag == 'latest':
            # Special case, return the latest commit from the repository.
            result = engine.run_sql(select("images", ','.join(IMAGE_COLS), "namespace = %s AND repository = %s")
                                    + SQL(" ORDER BY created DESC LIMIT 1"),
                                    (self.repository.namespace, self.repository.repository,),
                                    return_shape=ResultShape.ONE_MANY)
            if result is None:
                raise SplitGraphException("No commits found in %s!")
            return self._make_image(result)

        result = engine.run_sql(select("tags", "image_hash", "namespace = %s AND repository = %s AND tag = %s"),
                                (self.repository.namespace, self.repository.repository, tag),
                                return_shape=ResultShape.ONE_ONE)
        if result is None:
            if raise_on_none:
                schema = self.repository.to_schema()
                if tag == 'HEAD':
                    raise SplitGraphException(
                        "No current checked out revision found for %s. Check one out with \"sgr "
                        "checkout %s image_hash\"." % (schema, schema))
                else:
                    raise SplitGraphException("Tag %s not found in repository %s" % (tag, schema))
            else:
                return None
        return self.by_hash(result)

    def by_hash(self, image_hash, raise_on_none=True):
        """
        Returns an image corresponding to a given (possibly shortened) image hash. If the image hash
        is ambiguous, raises an error. If the image does not exist, raises an error or returns None.

        :param image_hash: Image hash (can be shortened).
        :param raise_on_none: Whether to raise if the image doesn't exist.
        :return: Image object or None
        """
        result = self.engine.run_sql(select("images", ','.join(IMAGE_COLS),
                                            "repository = %s AND image_hash LIKE %s AND namespace = %s"),
                                     (self.repository.repository, image_hash.lower() + '%',
                                      self.repository.namespace),
                                     return_shape=ResultShape.MANY_MANY)
        if not result:
            if raise_on_none:
                raise SplitGraphException("No images starting with %s found!" % image_hash)
            else:
                return None
        if len(result) > 1:
            result = "Multiple suitable candidates found: \n * " + "\n * ".join(result)
            raise SplitGraphException(result)
        return self._make_image(result[0])

    def __getitem__(self, key):
        """Resolve an Image object from its tag or hash."""
        # Things we can have here: full hash, shortened hash or tag.
        # Users can always use by_hash or by_tag to be explicit -- this is just a shorthand. There's little
        # chance for ambiguity (why would someone have a hexadecimal tag that can be confused with a hash?)
        # so we can detect what the user meant in the future.
        return self.by_tag(key, raise_on_none=False) or self.by_hash(key)

    def get_all_child_images(self, start_image):
        """Get all children of `start_image` of any degree."""
        all_images = self()
        result_size = 1
        result = {start_image}
        while True:
            # Keep expanding the set of children until it stops growing
            for image in all_images:
                if image.parent_id in result:
                    result.add(image.image_hash)
            if len(result) == result_size:
                return result
            result_size = len(result)

    def get_all_parent_images(self, start_images):
        """Get all parents of the 'start_images' set of any degree."""
        parent = {image.image_hash: image.parent_id for image in self()}
        result = set(start_images)
        result_size = len(result)
        while True:
            # Keep expanding the set of parents until it stops growing
            result.update({parent[image] for image in result if parent[image] is not None})
            if len(result) == result_size:
                return result
            result_size = len(result)

    def add(self, parent_id, image, created=None, comment=None, provenance_type=None, provenance_data=None):
        """
        Registers a new image in the Splitgraph image tree.

        Internal method used by actual image creation routines (committing, importing or pulling).

        :param parent_id: Parent of the image
        :param image: Image hash
        :param created: Creation time (defaults to current timestamp)
        :param comment: Comment (defaults to empty)
        :param provenance_type: Image provenance that can be used to rebuild the image
            (one of None, FROM, MOUNT, IMPORT, SQL)
        :param provenance_data: Extra provenance data (dictionary).
        """
        self.engine.run_sql(
            insert("images", ("image_hash", "namespace", "repository", "parent_id", "created", "comment",
                              "provenance_type", "provenance_data")),
            (image, self.repository.namespace, self.repository.repository, parent_id, created or datetime.now(),
             comment, provenance_type, Json(provenance_data)))

    def delete(self, images):
        """
        Deletes a set of Splitgraph images from the repository. Note this doesn't check whether
        this will orphan some other images in the repository and can make the state of the repository
        invalid.

        Image deletions won't be replicated on push/pull (those can only add new images).

        :param images: List of image IDs
        """
        if not images:
            return
        # Maybe better to have ON DELETE CASCADE on the FK constraints instead of going through
        # all tables to clean up -- but then we won't get alerted when we accidentally try
        # to delete something that does have FKs relying on it.
        args = tuple([self.repository.namespace, self.repository.repository] + list(images))
        for table in ['tags', 'tables', 'images']:
            self.engine.run_sql(SQL("DELETE FROM {}.{} WHERE namespace = %s AND repository = %s "
                                    "AND image_hash IN (" + ','.join(itertools.repeat('%s', len(images))) + ")")
                                .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(table)), args)

    def __iter__(self):
        return iter(self.engine.run_sql(select("images", "image_hash",
                                               "repository = %s AND namespace = %s"),
                                        (self.repository.repository, self.repository.namespace),
                                        return_shape=ResultShape.MANY_ONE))


class Repository:
    """
    Splitgraph repository API
    """

    def __init__(self, namespace, repository, engine=None):
        self.namespace = namespace
        self.repository = repository

        self.engine = engine or get_engine()
        ensure_metadata_schema(self.engine)
        self.images = ImageManager(self)

        # consider making this an injected/a singleton for a given engine
        # since it's global for the whole engine but calls (e.g. repo.objects.cleanup()) make it
        # look like it's the manager for objects related to a repo.
        self.objects = ObjectManager(self.engine)

    @classmethod
    def from_template(cls, template, namespace=None, repository=None, engine=None):
        """Create a Repository from an existing one replacing some of its attributes."""
        return cls(namespace or template.namespace, repository or template.repository, engine or template.engine)

    @classmethod
    def from_schema(cls, schema):
        """Convert a Postgres schema name of the format `namespace/repository` to a Splitgraph repository object."""
        if '/' in schema:
            namespace, repository = schema.split('/')
            return cls(namespace, repository)
        return cls('', schema)

    def __eq__(self, other):
        return self.namespace == other.namespace and self.repository == other.repository

    def to_schema(self):
        """Returns the engine schema that this repository gets checked out into."""
        return self.namespace + "/" + self.repository if self.namespace else self.repository

    def __repr__(self):
        return "Repository " + self.to_schema() + " on " + self.engine.name

    __str__ = to_schema

    def __hash__(self):
        return hash(self.namespace) * hash(self.repository)

    # --- GENERAL REPOSITORY MANAGEMENT ---

    @manage_audit
    def init(self):
        """
        Initializes an empty repo with an initial commit (hash 0000...)
        """
        self.engine.create_schema(self.to_schema())
        initial_image = '0' * 64
        self.engine.run_sql(insert("images", ("image_hash", "namespace", "repository", "parent_id", "created")),
                            (initial_image, self.namespace, self.repository, None, datetime.now()))
        # Strictly speaking this is redundant since the checkout (of the "HEAD" commit) updates the tag table.
        self.engine.run_sql(insert("tags", ("namespace", "repository", "image_hash", "tag")),
                            (self.namespace, self.repository, initial_image, "HEAD"))

    def rm(self, unregister=True, uncheckout=True):
        """
        Discards all changes to a given repository and optionally all of its history,
        as well as deleting the Postgres schema that it might be checked out into.
        Doesn't delete any cached physical objects.

        After performing this operation, this object becomes invalid and must be discarded,
        unless init() is called again.

        :param unregister: Whether to purge repository history/metadata
        :param uncheckout: Whether to delete the actual checked out repo
        """
        # Make sure to discard changes to this repository if they exist, otherwise they might
        # be applied/recorded if a new repository with the same name appears.
        if uncheckout:
            # If we're talking to a bare repo / a remote that doesn't have checked out repositories,
            # there's no point in touching the audit trigger.
            self.engine.discard_pending_changes(self.to_schema())

            # Dispose of the foreign servers (LQ FDW, other FDWs) for this schema if it exists (otherwise its connection
            # won't be recycled and we can get deadlocked).
            self.engine.run_sql(SQL("DROP SERVER IF EXISTS {} CASCADE").format(
                Identifier('%s_lq_checkout_server' % self.to_schema())))
            self.engine.run_sql(
                SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(self.to_schema() + '_server')))
            self.engine.run_sql(SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(self.to_schema())))

        if unregister:
            meta_tables = ["tables", "tags", "images"]
            if self.engine.table_exists(SPLITGRAPH_META_SCHEMA, 'upstream'):
                meta_tables.append("upstream")
            for meta_table in meta_tables:
                self.engine.run_sql(SQL("DELETE FROM {}.{} WHERE namespace = %s AND repository = %s")
                                    .format(Identifier(SPLITGRAPH_META_SCHEMA),
                                            Identifier(meta_table)),
                                    (self.namespace, self.repository))
        self.engine.commit()

    def get_upstream(self):
        """
        Gets the current upstream repository that a local repository tracks

        :return: Remote Repository object (with a remote engine)
        """
        result = self.engine.run_sql(select("upstream", "remote_name, remote_namespace, remote_repository",
                                            "namespace = %s AND repository = %s"),
                                     (self.namespace, self.repository),
                                     return_shape=ResultShape.ONE_MANY)
        if result is None:
            return result
        return Repository(namespace=result[1], repository=result[2], engine=get_engine(result[0]))

    def set_upstream(self, remote_repository):
        """
        Sets the upstream remote + repository that this repository tracks.

        :param remote_repository: Remote Repository object
        """
        self.engine.run_sql(SQL("INSERT INTO {0}.upstream (namespace, repository, "
                                "remote_name, remote_namespace, remote_repository) VALUES (%s, %s, %s, %s, %s)"
                                " ON CONFLICT (namespace, repository) DO UPDATE SET "
                                "remote_name = excluded.remote_name, remote_namespace = excluded.remote_namespace, "
                                "remote_repository = excluded.remote_repository WHERE "
                                "upstream.namespace = excluded.namespace "
                                "AND upstream.repository = excluded.repository")
                            .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                            (self.namespace, self.repository, remote_repository.engine.name,
                             remote_repository.namespace, remote_repository.repository))

    def delete_upstream(self):
        """
        Deletes the upstream remote + repository for a local repository.
        """
        self.engine.run_sql(SQL("DELETE FROM {0}.upstream WHERE namespace = %s AND repository = %s")
                            .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                            (self.namespace, self.repository),
                            return_shape=None)

    # --- COMMITS / CHECKOUTS ---

    @contextmanager
    def materialized_table(self, table_name, image_hash):
        """A context manager that returns a pointer to a read-only materialized table in a given image.
        If the table is already stored as a SNAP, this doesn't use any extra space.
        Otherwise, the table is materialized and deleted on exit from the context manager.

        :param table_name: Name of the table
        :param image_hash: Image hash to materialize
        :return: (schema, table_name) where the materialized table is located.
            The table must not be changed, as it might be a pointer to a real SG SNAP object.
        """
        if image_hash is None:
            # No image hash -- just return the current staging table.
            yield self.to_schema(), table_name
            return  # make sure we don't fall through after the user is finished

        table = self.images.by_hash(image_hash).get_table(table_name)
        with self.objects.ensure_objects(table) as required_objects:
            if len(required_objects) > 1:
                new_id = get_random_object_id()
                table.materialize(new_id, destination_schema=SPLITGRAPH_META_SCHEMA)
                try:
                    yield SPLITGRAPH_META_SCHEMA, new_id
                finally:
                    # Maybe some cache management/expiry strategies here
                    self.objects.delete_objects([new_id])
            else:
                yield SPLITGRAPH_META_SCHEMA, required_objects[0]

    @property
    def head(self):
        """Return the HEAD image for the repository or None if the repository isn't checked out."""
        return self.images.by_tag('HEAD', raise_on_none=False)

    @manage_audit
    def uncheckout(self, force=False):
        """
        Deletes the schema that the repository is checked out into

        :param force: Discards all pending changes to the schema.
        """
        if self.has_pending_changes():
            if not force:
                raise SplitGraphException("{0} has pending changes! Pass force=True or do sgr checkout -f {0}:HEAD"
                                          .format(self.to_schema()))
            logging.warning("%s has pending changes, discarding...", self.to_schema())

        # Delete the schema and remove the HEAD tag
        self.rm(unregister=False, uncheckout=True)
        self.head.delete_tag('HEAD')

    def commit(self, image_hash=None, comment=None, snap_only=False):
        """
        Commits all pending changes to a given repository, creating a new image.

        :param image_hash: Hash of the commit. Chosen by random if unspecified.
        :param comment: Optional comment to add to the commit.
        :param snap_only: If True, will store the table as a full snapshot instead of delta compression
        :return: The newly created Image object.
        """

        logging.info("Committing %s...", self.to_schema())

        ensure_metadata_schema(self.engine)
        self.engine.commit()
        manage_audit_triggers(self.engine)

        # HEAD can be None (if this is the first commit in this repository)
        head = self.head
        if image_hash is None:
            image_hash = "%0.2x" % getrandbits(256)

        self.images.add(head.image_hash if head else None, image_hash, comment=comment)

        # TODO TF work: probably disallow tables being stored as both snaps and diffs since it adds
        # a lot of complexity to the schema (an object can have multiple parents).

        self._commit(head, image_hash, snap_only=snap_only)

        set_head(self, image_hash)
        manage_audit_triggers(self.engine)
        self.engine.commit()
        return self.images.by_hash(image_hash)

    def _commit(self, head, image_hash, snap_only=False):
        """
        Reads the recorded pending changes to all tables in a given mountpoint, conflates them and possibly stores them
        as new object(s) as follows:

            * If a table has been created or there has been a schema change, it's only stored as a SNAP (full snapshot).
            * If a table hasn't changed since the last revision, no new objects are created and it's linked to the
                previous objects belonging to the last revision.
            * Otherwise, the table is stored as a conflated (1 change per PK) DIFF object and an optional SNAP.

        :param head: Current HEAD image to base the commit on.
        :param image_hash: Hash of the image to commit changes under.
        :param snap_only: If True, only stores the table as a SNAP.
        """
        target_schema = self.to_schema()

        changed_tables = self.engine.get_changed_tables(target_schema)
        for table in self.engine.get_all_tables(target_schema):
            table_info = head.get_table(table) if head else None
            # Table already exists at the current HEAD
            if not table_info or snap_only:
                self.objects.record_table_as_snap(self, table, image_hash)
                continue

            # If there has been a schema change, we currently just snapshot the whole table.
            # This is obviously wasteful (say if just one column has been added/dropped or we added a PK,
            # but it's a starting point to support schema changes.
            if table_info.table_schema != self.engine.get_full_table_schema(self.to_schema(), table):
                # TODO TF work: this mostly makes sense, if a table is new, we chunk it up and store as
                # multiple objects
                self.objects.record_table_as_snap(self, table, image_hash)
                continue

            if table in changed_tables:
                self.objects.record_table_as_diff(table_info, image_hash)
                continue

            # If the table wasn't changed, point the image to the old table
            self.objects.register_table(self, table, image_hash, table_info.table_schema, table_info.objects)

        # Make sure that all pending changes have been discarded by this point (e.g. if we created just a snapshot for
        # some tables and didn't consume the audit log).
        # NB if we allow partial commits, this will have to be changed (only discard for committed tables).
        self.engine.discard_pending_changes(target_schema)

    def has_pending_changes(self):
        """
        Detects if the repository has any pending changes (schema changes, table additions/deletions, content changes).
        """
        head = self.head
        if not head:
            # If the repo isn't checked out, no point checking for changes.
            return False
        for table in self.engine.get_all_tables(self.to_schema()):
            if self.diff(table, head.image_hash, None, aggregate=True) != (0, 0, 0):
                return True
        return False

    # --- TAG AND IMAGE MANAGEMENT ---

    def get_all_hashes_tags(self):
        """
        Gets all tagged images and their hashes in a given repository.

        :return: List of (image_hash, tag)
        """
        return self.engine.run_sql(select("tags", "image_hash, tag", "namespace = %s AND repository = %s"),
                                   (self.namespace, self.repository,))

    def set_tags(self, tags, force=False):
        """
        Sets tags for multiple images.

        :param tags: List of (image_hash, tag)
        :param force: Whether to remove the old tag if an image with this tag already exists.
        """
        for tag, image_id in tags.items():
            if tag != 'HEAD':
                self.images.by_hash(image_id).tag(tag, force)

    def run_sql(self, sql, arguments=None, return_shape=ResultShape.MANY_MANY):
        """Execute an arbitrary SQL statement inside of this repository's checked out schema."""
        self.engine.run_sql("SET search_path TO %s", (self.to_schema(),))
        result = self.engine.run_sql(sql, arguments=arguments, return_shape=return_shape)
        self.engine.run_sql("SET search_path TO public")
        return result

    def dump(self, stream):
        """
        Creates an SQL dump with the metadata required for the repository and all of its objects.

        :param stream: Stream to dump the data into.
        """
        # First, go through the metadata tables required to reconstruct the repository.
        stream.write("""--\n-- Metadata tables --\n--\n""")
        self.engine.dump_table_sql(SPLITGRAPH_META_SCHEMA, 'images', stream, where="namespace = %s AND repository = %s",
                                   where_args=(self.namespace, self.repository))
        self.engine.dump_table_sql(SPLITGRAPH_META_SCHEMA, 'tables', stream, where="namespace = %s AND repository = %s",
                                   where_args=(self.namespace, self.repository))
        self.engine.dump_table_sql(SPLITGRAPH_META_SCHEMA, 'tags', stream,
                                   where="namespace = %s AND repository = %s AND tag != 'HEAD'",
                                   where_args=(self.namespace, self.repository))

        # Get required objects
        required_objects = set()
        for image_hash in self.images:
            image = self.images.by_hash(image_hash)
            for table_name in image.get_tables():
                for object_id in image.get_table(table_name).objects:
                    required_objects.add(object_id)

        # Expand the required objects into a full set
        all_required_objects = set()
        for object_id in required_objects:
            all_required_objects.update(self.objects.get_all_required_objects(object_id))

        object_qual = "object_id IN (" + ",".join(itertools.repeat('%s', len(all_required_objects))) + ")"

        stream.write("""--\n-- Object metadata --\n--\n""")
        # To avoid conflicts, we just delete the object records if they already exist
        with self.engine.connection.cursor() as cur:
            for table_name in ("objects", "object_locations"):
                stream.write(cur.mogrify(SQL("DELETE FROM {}.{} WHERE ")
                                         .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(table_name))
                                         + SQL(object_qual), list(all_required_objects)).decode('utf-8'))
                stream.write(";\n\n")
                self.engine.dump_table_sql(SPLITGRAPH_META_SCHEMA, table_name, stream, where=object_qual,
                                           where_args=list(all_required_objects))

            stream.write("""--\n-- Object contents --\n--\n""")

            # Finally, dump the actual objects
            for object_id in all_required_objects:
                stream.write(cur.mogrify(SQL("DROP TABLE IF EXISTS {}.{};\n")
                                         .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)))
                             .decode('utf-8'))
                stream.write(cur.mogrify(
                    self.engine.dump_table_creation(schema=SPLITGRAPH_META_SCHEMA, tables=[object_id],
                                                    created_schema=SPLITGRAPH_META_SCHEMA))
                             .decode('utf-8'))
                stream.write(";\n")
                self.engine.dump_table_sql(SPLITGRAPH_META_SCHEMA, object_id, stream)
                stream.write("\n")

    # --- IMPORTING TABLES ---

    @manage_audit
    def import_tables(self, tables, source_repository, source_tables, image_hash=None, foreign_tables=False,
                      do_checkout=True, target_hash=None, table_queries=None):
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
        :param foreign_tables: If True, copies all source tables to create a series of new SNAP objects instead of
            treating them as Splitgraph-versioned tables. This is useful for adding brand new tables
            (for example, from an FDW-mounted table).
        :param do_checkout: If False, doesn't materialize the tables in the target mountpoint.
        :param target_hash: Hash of the new image that tables is recorded under. If None, gets chosen at random.
        :param table_queries: If not [], it's treated as a Boolean mask showing which entries in the `tables` list are
            instead SELECT SQL queries that form the target table. The queries have to be non-schema qualified and work
            only against tables in the source repository. Each target table created is the result of the respective SQL
            query. This is committed as a new snapshot.
        :return: Hash that the new image was stored under.
        """
        # Sanitize/validate the parameters and call the internal function.
        if table_queries is None:
            table_queries = []
        target_hash = target_hash or "%0.2x" % getrandbits(256)

        if not foreign_tables:
            image = source_repository.images.by_hash(image_hash) if image_hash else source_repository.head
        else:
            image = None

        if not source_tables:
            source_tables = image.get_tables() if not foreign_tables \
                else source_repository.engine.get_all_tables(source_repository.to_schema())
        if not tables:
            if table_queries:
                raise ValueError("target_tables has to be defined if table_queries is True!")
            tables = source_tables
        if not table_queries:
            table_queries = [False] * len(tables)
        if len(tables) != len(source_tables) or len(source_tables) != len(table_queries):
            raise ValueError("tables, source_tables and table_queries have mismatching lengths!")

        existing_tables = self.engine.get_all_tables(self.to_schema())
        clashing = [t for t in tables if t in existing_tables]
        if clashing:
            raise ValueError("Table(s) %r already exist(s) at %s!" % (clashing, self))

        return self._import_tables(image, tables, source_repository, target_hash, source_tables, do_checkout,
                                   table_queries, foreign_tables)

    def _import_tables(self, image, tables, source_repository, target_hash, source_tables, do_checkout,
                       table_queries, foreign_tables):
        engine = self.engine
        engine.create_schema(self.to_schema())

        # This importing route only supported between local repos.
        assert engine == source_repository.engine

        head = self.head
        self.images.add(head.image_hash if head else None, target_hash,
                        comment="Importing %s from %s" % (tables, source_repository))

        if any(table_queries) and not foreign_tables:
            # If we want to run some queries against the source repository to create the new tables,
            # we have to materialize it fully.

            # TODO TF work: try to LQ here since then we might download fewer tables
            image.checkout()

        # Materialize the actual tables in the target repository and register them.
        for source_table, target_table, is_query in zip(source_tables, tables, table_queries):
            if foreign_tables or is_query:
                # For foreign tables/SELECT queries, we define a new object/table instead.
                object_id = get_random_object_id()
                if is_query:
                    # is_query precedes foreign_tables: if we're importing using a query, we don't care if it's a
                    # foreign table or not since we're storing it as a full snapshot.
                    engine.run_sql_in(source_repository.to_schema(),
                                      SQL("CREATE TABLE {}.{} AS ").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                           Identifier(object_id))
                                      + SQL(source_table))
                elif foreign_tables:
                    self.engine.copy_table(source_repository.to_schema(), source_table, SPLITGRAPH_META_SCHEMA,
                                           object_id)
                # TODO TF work this is where a lot of space wasting will come from; we should probably
                # also do actual object hashing to avoid duplication for things like SELECT *

                # Might not be necessary if we don't actually want to materialize the snapshot (wastes space).
                self.objects.register_object(object_id, 'SNAP', namespace=self.namespace,
                                             parent_object=None)
                self.objects.register_table(self, target_table, target_hash,
                                            self.engine.get_full_table_schema(SPLITGRAPH_META_SCHEMA, object_id),
                                            [object_id])
                if do_checkout:
                    self.engine.copy_table(SPLITGRAPH_META_SCHEMA, object_id, self.to_schema(), target_table)
            else:
                # TODO TF work: same here, just link to the same objects
                table_obj = image.get_table(source_table)
                self.objects.register_table(self, target_table, target_hash, table_obj.table_schema, table_obj.objects)
                if do_checkout:
                    table_obj.materialize(target_table, destination_schema=self.to_schema())
        # Register the existing tables at the new commit as well.
        if head is not None:
            # Maybe push this into the tables API (currently have to make 2 queries)
            engine.run_sql(SQL("""INSERT INTO {0}.tables (namespace, repository, image_hash, 
                    table_name, table_schema, object_ids) (SELECT %s, %s, %s, table_name, table_schema, object_ids
                    FROM {0}.tables WHERE namespace = %s AND repository = %s AND image_hash = %s)""")
                           .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                           (self.namespace, self.repository, target_hash,
                            self.namespace, self.repository, head.image_hash))
        set_head(self, target_hash)
        return target_hash

    # --- SYNCING WITH OTHER REPOSITORIES ---

    def _sync(self, source, download=True, download_all=False, handler='DB', handler_options=None):
        """
        Generic routine for syncing two repositories: fetches images, hashes, objects and tags
        on `source` that don't exist in this repository.

        Common code between push and pull, since the only difference between those routines is that
        uploading and downloading objects are different operations.

        :param source: Source Repository object
        :param download: If True, uses the download routines to download physical objects to self.
            If False, uses the upload routines to get `source` to upload physical objects to self / external.
        :param download_all: Whether to download all objects (pull option)
        :param handler: Upload handler
        :param handler_options: Upload handler options
        """

        if handler_options is None:
            handler_options = {}

        # Get the remote log and the list of objects we need to fetch.
        logging.info("Gathering remote metadata...")
        new_images, table_meta, object_locations, object_meta, tags = \
            gather_sync_metadata(self, source)

        if not new_images:
            logging.info("Nothing to do.")
            return

        if download:
            # Don't actually download any real objects until the user tries to check out a revision.
            if download_all:
                # Check which new objects we need to fetch/preregister.
                # We might already have some objects prefetched
                # (e.g. if a new version of the table is the same as the old version)
                logging.info("Fetching remote objects...")
                self.objects.download_objects(source.objects,
                                              objects_to_fetch=list(set(o[0] for o in object_meta)),
                                              object_locations=object_locations)

            self.objects.register_objects(object_meta)
            self.objects.register_object_locations(object_locations)
            # Don't check anything out, keep the repo bare.
            set_head(self, None)
        else:
            new_uploads = source.objects.upload_objects(self.objects, list(set(o[0] for o in object_meta)),
                                                        handler=handler, handler_params=handler_options)
            # Here we have to register the new objects after the upload but before we store their external
            # location (as the RLS for object_locations relies on the object metadata being in place)
            self.objects.register_objects(object_meta, namespace=self.namespace)
            self.objects.register_object_locations(object_locations + new_uploads)
            source.objects.register_object_locations(new_uploads)

        # Register the new tables / tags.
        self.objects.register_tables(self, table_meta)
        self.set_tags(tags, force=False)

        print(("Fetched" if download else "Uploaded") +
              " metadata for %d object(s), %d table version(s) and %d tag(s)." % (len(object_meta), len(table_meta),
                                                                                  len([t for t in tags if
                                                                                       t != 'HEAD'])))

    def push(self, remote_repository=None, handler='DB', handler_options=None):
        """
        Inverse of ``pull``: Pushes all local changes to the remote and uploads new objects.

        :param remote_repository: Remote repository to push changes to. If not specified, the current
            upstream is used.
        :param handler: Name of the handler to use to upload objects. Use `DB` to push them to the remote or `S3`
            to store them in an S3 bucket.
        :param handler_options: Extra options to pass to the handler. For example, see
            :class:`splitgraph.hooks.s3.S3ExternalObjectHandler`.
        """
        ensure_metadata_schema(self.engine)
        # Maybe consider having a context manager for getting a remote engine instance
        # that auto-commits/closes when needed?
        remote_repository = remote_repository or self.get_upstream()
        if not remote_repository:
            raise SplitGraphException("No remote repository specified and no upstream found for %s!" % self.to_schema())

        try:
            remote_repository._sync(source=self, download=False, handler=handler,
                                    handler_options=handler_options)

            if self.get_upstream() is None:
                self.set_upstream(remote_repository)
        finally:
            remote_repository.engine.commit()
            remote_repository.engine.close()
        return remote_repository

    def pull(self, download_all=False):
        """
        Synchronizes the state of the local Splitgraph repository with its upstream, optionally downloading all new
        objects created on the remote.

        :param download_all: If True, downloads all objects and stores them locally. Otherwise, will only download
            required objects when a table is checked out.
        """
        upstream = self.get_upstream()
        if not upstream:
            raise SplitGraphException("No upstream found for repository %s!" % self.to_schema())

        clone(remote_repository=upstream, local_repository=self, download_all=download_all)

    def publish(self, tag, remote_repository=None, readme="", include_provenance=True,
                include_table_previews=True):
        """
        Summarizes the data on a previously-pushed repository and makes it available in the catalog.

        :param tag: Image tag. Only images with tags can be published.
        :param remote_repository: Remote Repository object (uses the upstream if unspecified)
        :param readme: Optional README for the repository.
        :param include_provenance: If False, doesn't include the dependencies of the image
        :param include_table_previews: Whether to include data previews for every table in the image.
        """
        remote_repository = remote_repository or self.get_upstream()
        if not remote_repository:
            raise SplitGraphException("No remote repository specified and no upstream found for %s!" % self.to_schema())

        image = self.images[tag]
        logging.info("Publishing %s:%s (%s)", self, image.image_hash, tag)

        dependencies = [((r.namespace, r.repository), i) for r, i in image.provenance()] \
            if include_provenance else None
        previews, schemata = prepare_publish_data(image, self, include_table_previews)

        try:
            publish_tag(remote_repository, tag, image.image_hash, datetime.now(), dependencies, readme,
                        schemata=schemata, previews=previews if include_table_previews else None)
            remote_repository.engine.commit()
        finally:
            remote_repository.engine.close()

    def diff(self, table_name, image_1, image_2, aggregate=False):
        """
        Compares the state of a table in different images. If the two images are on the same path in the commit tree,
        it doesn't need to materialize any of the tables and simply aggregates their DIFF objects to produce a complete
        changelog. Otherwise, it materializes both tables into a temporary space and compares them row-to-row.

        :param table_name: Name of the table.
        :param image_1: First image hash / object. If None, uses the state of the current staging area.
        :param image_2: Second image hash / object. If None, uses the state of the current staging area.
        :param aggregate: If True, returns a tuple of integers denoting added, removed and updated rows between
            the two images.
        :return: If the table doesn't exist in one of the images, returns True if it was added and False if it was
            removed.
            If `aggregate` is True, returns the aggregation of changes as specified before.
            Otherwise, returns a list of changes where each change is of the format
            `(primary key, action_type, action_data)`:

                * `action_type == 0` is Insert and the `action_data` contains a dictionary of non-PK columns and values
                    inserted.
                * `action_type == 1`: Delete, `action_data` is None.
                * `action_type == 2`: Update, `action_data` is a dictionary of non-PK columns and their new values for
                    that particular row.
        """

        if isinstance(image_1, str):
            image_1 = self.images.by_hash(image_1)
        if isinstance(image_2, str):
            image_2 = self.images.by_hash(image_2)

        # If the table doesn't exist in the first or the second image, short-circuit and
        # return the bool.
        if not table_exists_at(self, table_name, image_1):
            return True
        if not table_exists_at(self, table_name, image_2):
            return False

        # Special case: if diffing HEAD and staging (with aggregation), we can return that directly.
        if image_1 == self.head and image_2 is None and aggregate:
            return aggregate_changes(self.engine.get_pending_changes(self.to_schema(), table_name, aggregate=True))

        # If the table is the same in the two images, short circuit as well.
        if image_2 is not None:
            if set(image_1.get_table(table_name).objects) == \
                    set(image_2.get_table(table_name).objects):
                return [] if not aggregate else (0, 0, 0)

        # Materialize both tables and compare them side-by-side.
        # TODO we can aggregate chunks in a similar way that LQ does it.
        return slow_diff(self, table_name, _hash(image_1), _hash(image_2), aggregate)


def import_table_from_remote(remote_repository, remote_tables, remote_image_hash, target_repository, target_tables,
                             target_hash=None):
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
    tmp_mountpoint = Repository(namespace=remote_repository.namespace,
                                repository=remote_repository.repository + '_clone_tmp')

    clone(remote_repository, local_repository=tmp_mountpoint, download_all=False)
    target_repository.import_tables(target_tables, tmp_mountpoint, remote_tables, image_hash=remote_image_hash,
                                    target_hash=target_hash)

    tmp_mountpoint.rm()
    target_repository.engine.commit()


def find_path(repository, hash_1, hash_2):
    """If the two images are on the same path in the commit tree, returns that path."""
    path = []
    while hash_2 is not None:
        path.append(hash_2)
        hash_2 = repository.images.by_hash(hash_2).parent_id
        if hash_2 == hash_1:
            return path


def table_exists_at(repository, table_name, image=None):
    """Determines whether a given table exists in a Splitgraph image without checking it out. If `image_hash` is None,
    determines whether the table exists in the current staging area."""
    return repository.engine.table_exists(repository.to_schema(), table_name) if image is None \
        else bool(image.get_table(table_name))


def clone(remote_repository, local_repository=None, download_all=False):
    """
    Clones a remote Splitgraph repository or synchronizes remote changes with the local ones.

    If the target repository has no set upstream engine, the source repository becomes its upstream.

    :param remote_repository: Remote Repository object to clone or the repository's name. If a name is passed,
        the repository will be looked up on the current lookup path in order to find the engine the repository
        belongs to.
    :param local_repository: Local repository to clone into. If None, uses the same name as the remote.
    :param download_all: If True, downloads all objects and stores them locally. Otherwise, will only download required
        objects when a table is checked out.
    :return: A locally cloned Repository object.
    """
    if isinstance(remote_repository, str):
        remote_repository = lookup_repository(remote_repository, include_local=False)

    # Repository engine should be local by default
    if not local_repository:
        local_repository = Repository(remote_repository.namespace, remote_repository.repository)

    local_repository._sync(remote_repository, download=True, download_all=download_all)

    if local_repository.get_upstream() is None:
        local_repository.set_upstream(remote_repository)

    return local_repository


def _hash(image):
    return image.image_hash if image is not None else None
