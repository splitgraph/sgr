import logging
from contextlib import contextmanager
from datetime import datetime
from random import getrandbits

from psycopg2.sql import SQL, Identifier

import splitgraph
from splitgraph import SplitGraphException, publish_tag
from splitgraph._data.images import add_new_image
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core._common import select, insert, ensure_metadata_schema
from splitgraph.core.engine import repository_exists, lookup_repo
from splitgraph.core.object_manager import ObjectManager
from splitgraph.engine import ResultShape, get_engine
from ._common import manage_audit_triggers, set_head, manage_audit
from ._objects.creation import record_table_as_snap, record_table_as_diff
from ._objects.utils import get_random_object_id
from .image import Image, IMAGE_COLS

_PUBLISH_PREVIEW_SIZE = 100


# OO API porting plan:
# [ ] move things out from Repository into more appropriate classes
# [ ] review current internal metadata access/creation commands to see which can be made into private methods
# [ ] clone() and push() are very similar now, merge with some extra UX flags ("uploading"/"downloading")
# [ ] Document the new API, regenerate the docs


class Repository:
    """
    Splitgraph repository API
    """

    def __init__(self, namespace, repository, engine=None):
        self.namespace = namespace
        self.repository = repository

        self.engine = engine or splitgraph.get_engine()

        # consider making this an injected/a singleton for a given engine
        # since it's global for the whole engine but calls (e.g. repo.objects.cleanup()) make it
        # look like it's the manager for objects related to a repo.
        self.objects = ObjectManager(self.engine)

    def switch_engine(self, engine):
        """Used instead of creating a new Repository object with a different engine or doing
        repository.engine = new_engine (since then the object manager isn't repointed)"""
        self.engine = engine
        self.objects = ObjectManager(engine)

    def __eq__(self, other):
        return self.namespace == other.namespace and self.repository == other.repository

    def to_schema(self):
        return self.namespace + "/" + self.repository if self.namespace else self.repository

    def __repr__(self):
        return "Repository " + self.to_schema() + " on " + self.engine.name

    __str__ = to_schema

    def __hash__(self):
        return hash(self.namespace) * hash(self.repository)

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
        ensure_metadata_schema(self.engine)
        if uncheckout:
            # If we're talking to a bare repo / a remote that doesn't have checked out repositories,
            # there's no point in touching the audit trigger.
            self.engine.discard_pending_changes(self.to_schema())
            self.engine.run_sql(SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(self.to_schema())))
            # Drop server too if it exists (could have been a non-foreign repository)
            self.engine.run_sql(
                SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(self.to_schema() + '_server')))

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
        # See if the table snapshot already exists, otherwise reconstruct it
        table = self.get_image(image_hash).get_table(table_name)
        object_id = table.get_object('SNAP')
        if object_id is None:
            # Materialize the SNAP into a new object
            new_id = get_random_object_id()
            table.materialize(new_id, destination_schema=SPLITGRAPH_META_SCHEMA)
            yield SPLITGRAPH_META_SCHEMA, new_id
            # Maybe some cache management/expiry strategies here
            self.objects.delete_objects([new_id])
        else:
            if self.engine.table_exists(SPLITGRAPH_META_SCHEMA, object_id):
                yield SPLITGRAPH_META_SCHEMA, object_id
            else:
                # The SNAP object doesn't actually exist remotely, so we have to download it.
                # An optimisation here: we could open an RO connection to the remote instead if the object
                # does live there.
                upstream = self.get_upstream()
                if not upstream:
                    raise SplitGraphException("SNAP %s from %s doesn't exist locally and no remote was found for it!"
                                              % (object_id, str(self)))
                object_locations = self.objects.get_external_object_locations([object_id])
                self.objects.download_objects(upstream.engine, objects_to_fetch=[object_id],
                                              object_locations=object_locations)
                yield SPLITGRAPH_META_SCHEMA, object_id

                self.objects.delete_objects([object_id])

    @manage_audit
    def checkout(self, image_hash=None, tag=None, tables=None, keep_downloaded_objects=True, force=False):
        """
        Checks out an image belonging to a given repository, changing the current HEAD pointer. Raises an error
        if there are pending changes to its checkout.

        :param image_hash: Hash of the image to check out.
        :param tag: Tag of the image to check out. One of `image_hash` or `tag` must be specified.
        :param tables: List of tables to materialize in the mountpoint.
        :param keep_downloaded_objects: If False, deletes externally downloaded objects after they've been used.
        :param force: Discards all pending changes to the schema.
        """
        target_schema = self.to_schema()
        if self.has_pending_changes():
            if not force:
                raise SplitGraphException("{0} has pending changes! Pass force=True or do sgr checkout -f {0}:HEAD"
                                          .format(self.to_schema()))
            logging.warning("%s has pending changes, discarding...", self.to_schema())
            self.engine.discard_pending_changes(target_schema)
        # Detect the actual image
        if image_hash:
            # get_canonical_image_hash called twice if the commandline entry point already called it. How to fix?
            image_hash = self.get_canonical_image_id(image_hash)
        elif tag:
            image_hash = self.get_tagged_id(tag)
        else:
            raise SplitGraphException("One of image_hash or tag must be specified!")

        image = self.get_image(image_hash)
        # Drop all current tables in staging
        self.engine.create_schema(target_schema)
        for table in self.engine.get_all_tables(target_schema):
            self.engine.delete_table(target_schema, table)

        downloaded_object_ids = set()
        tables = tables or image.get_tables()
        for table in tables:
            downloaded_object_ids |= image.get_table(table).materialize(table)

        # Repoint the current HEAD for this repository to the new snap ID
        set_head(self, image_hash)

        if not keep_downloaded_objects:
            logging.info("Removing %d downloaded objects from cache...", len(downloaded_object_ids))
            self.objects.delete_objects(downloaded_object_ids)

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
        self.get_image(self.get_head()).delete_tag('HEAD')

    def commit(self, image_hash=None, include_snap=False, comment=None):
        """
        Commits all pending changes to a given repository, creating a new image.

        :param image_hash: Hash of the commit. Chosen by random if unspecified.
        :param include_snap: If True, also creates a SNAP object with a full copy of the table. This will speed up
            checkouts, but consumes extra space.
        :param comment: Optional comment to add to the commit.
        :return: The image hash the current state of the repository was committed under.
        """
        logging.info("Committing %s...", self.to_schema())

        ensure_metadata_schema(self.engine)
        self.engine.commit()
        manage_audit_triggers(self.engine)

        # HEAD can be None (if this is the first commit in this repository)
        head = self.get_head(raise_on_none=False)

        if image_hash is None:
            image_hash = "%0.2x" % getrandbits(256)

        # Add the new snap ID to the tree
        add_new_image(self, head, image_hash, comment=comment)

        self._commit(head, image_hash, include_snap=include_snap)

        set_head(self, image_hash)
        manage_audit_triggers(self.engine)
        self.engine.commit()
        return image_hash

    def _commit(self, current_head, image_hash, include_snap=False):
        """
        Reads the recorded pending changes to all tables in a given mountpoint, conflates them and possibly stores them
        as new object(s) as follows:

            * If a table has been created or there has been a schema change, it's only stored as a SNAP (full snapshot).
            * If a table hasn't changed since the last revision, no new objects are created and it's linked to the previous
              objects belonging to the last revision.
            * Otherwise, the table is stored as a conflated (1 change per PK) DIFF object and an optional SNAP.

        :param current_head: Current HEAD pointer to base the commit on.
        :param image_hash: Hash of the image to commit changes under.
        :param include_snap: If True, also stores the table as a SNAP.
        """
        target_schema = self.to_schema()

        head = self.get_image(current_head) if current_head else None

        changed_tables = self.engine.get_changed_tables(target_schema)
        for table in self.engine.get_all_tables(target_schema):
            table_info = head.get_table(table) if head else None
            # Table already exists at the current HEAD

            if table_info:
                # If there has been a schema change, we currently just snapshot the whole table.
                # This is obviously wasteful (say if just one column has been added/dropped or we added a PK,
                # but it's a starting point to support schema changes.
                snap_1 = self.objects.get_image_object_path(table_info)[0]
                if self.engine.get_full_table_schema(SPLITGRAPH_META_SCHEMA, snap_1) != \
                        self.engine.get_full_table_schema(self.to_schema(), table):
                    record_table_as_snap(table_info, image_hash)
                    continue

                if table in changed_tables:
                    record_table_as_diff(table_info, image_hash)
                else:
                    # If the table wasn't changed, point the commit to the old table objects (including
                    # any of snaps or diffs).
                    # This feels slightly weird: are we denormalized here?
                    for prev_object_id, _ in table_info.objects:
                        self.objects.register_table(self, table, image_hash, prev_object_id)

            # If table created (or we want to store a snap anyway), copy the whole table over as well.
            if not table_info or include_snap:
                record_table_as_snap(table_info, image_hash, table_name=table, repository=self)

        # Make sure that all pending changes have been discarded by this point (e.g. if we created just a snapshot for
        # some tables and didn't consume the audit log).
        # NB if we allow partial commits, this will have to be changed (only discard for committed tables).
        self.engine.discard_pending_changes(target_schema)

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
                self.get_image(image_id).tag(tag, force)

    def get_tagged_id(self, tag, raise_on_none=True):
        """
        Returns the image hash with a given tag in a given repository.

        :param tag: Tag. 'latest' is a special case: it returns the most recent image in the repository.
        :param raise_on_none: Whether to raise an error or return None if the repository isn't checked out.
        """
        engine = self.engine
        ensure_metadata_schema(engine)
        if not repository_exists(self) and raise_on_none:
            raise SplitGraphException("%s does not exist!" % str(self))

        if tag == 'latest':
            # Special case, return the latest commit from the repository.
            result = engine.run_sql(select("images", "image_hash", "namespace = %s AND repository = %s")
                                    + SQL(" ORDER BY created DESC LIMIT 1"), (self.namespace, self.repository,),
                                    return_shape=ResultShape.ONE_ONE)
            if result is None:
                raise SplitGraphException("No commits found in %s!")
            return result

        result = engine.run_sql(select("tags", "image_hash", "namespace = %s AND repository = %s AND tag = %s"),
                                (self.namespace, self.repository, tag),
                                return_shape=ResultShape.ONE_ONE)
        if result is None:
            if raise_on_none:
                schema = self.to_schema()
                if tag == 'HEAD':
                    raise SplitGraphException("No current checked out revision found for %s. Check one out with \"sgr "
                                              "checkout %s image_hash\"." % (schema, schema))
                else:
                    raise SplitGraphException("Tag %s not found in repository %s" % (tag, schema))
            else:
                return None
        return result

    def get_canonical_image_id(self, short_image):
        """
        Converts a truncated image hash into a full hash. Raises if there's an ambiguity.

        :param short_image: Shortened image hash
        """
        candidates = self.engine.run_sql(select("images", "image_hash",
                                                "namespace = %s AND repository = %s AND image_hash LIKE %s"),
                                         (self.namespace, self.repository, short_image.lower() + '%'),
                                         return_shape=ResultShape.MANY_ONE)

        if not candidates:
            raise SplitGraphException("No snapshots beginning with %s found for mountpoint %s!" % (short_image,
                                                                                                   self.to_schema()))

        if len(candidates) > 1:
            result = "Multiple suitable candidates found: \n * " + "\n * ".join(candidates)
            raise SplitGraphException(result)

        return candidates[0]

    def resolve_image(self, tag_or_hash, raise_on_none=True):
        """Converts a tag or shortened hash to a full image hash that exists in the repository."""
        try:
            return self.get_canonical_image_id(tag_or_hash)
        except SplitGraphException:
            try:
                return self.get_tagged_id(tag_or_hash)
            except SplitGraphException:
                if raise_on_none:
                    raise SplitGraphException(
                        "%s does not refer to either an image commit hash or a tag!" % tag_or_hash)
                else:
                    return None

    @manage_audit
    def import_tables(self, tables, target_repository, target_tables, image_hash=None, foreign_tables=False,
                      do_checkout=True, target_hash=None, table_queries=None):
        """
        Creates a new commit in target_mountpoint with one or more tables linked to already-existing tables.
        After this operation, the HEAD of the target mountpoint moves to the new commit and the new tables are materialized.

        :param tables: List of tables to import. If empty, imports all tables.
        :param target_repository: Mountpoint to import tables into.
        :param target_tables: If not empty, must be the list of the same length as `tables` specifying names to store them
            under in the target mountpoint.
        :param image_hash: Commit hash on the source mountpoint to import tables from.
            Uses the current source HEAD by default.
        :param foreign_tables: If True, copies all source tables to create a series of new SNAP objects instead of treating
            them as Splitgraph-versioned tables. This is useful for adding brand new tables
            (for example, from an FDW-mounted table).
        :param do_checkout: If False, doesn't materialize the tables in the target mountpoint.
        :param target_hash: Hash of the new image that tables is recorded under. If None, gets chosen at random.
        :param table_queries: If not [], it's treated as a Boolean mask showing which entries in the `tables` list are
            instead SELECT SQL queries that form the target table. The queries have to be non-schema qualified and work only
            against tables in the source mountpoint. Each target table created is the result of the respective SQL query.
            This is committed as a new snapshot.
        :return: Hash that the new image was stored under.
        """
        # TODO import: should this be the method of the source repo/image instead?

        # Sanitize/validate the parameters and call the internal function.
        if table_queries is None:
            table_queries = []
        target_hash = target_hash or "%0.2x" % getrandbits(256)

        if not foreign_tables:
            image_hash = image_hash or self.get_head()

        if not tables:
            tables = self.get_image(image_hash).get_tables() if not foreign_tables \
                else self.engine.get_all_tables(self.to_schema())
        if not target_tables:
            if table_queries:
                raise ValueError("target_tables has to be defined if table_queries is True!")
            target_tables = tables
        if not table_queries:
            table_queries = [False] * len(tables)
        if len(tables) != len(target_tables) or len(tables) != len(table_queries):
            raise ValueError("tables, target_tables and table_queries have mismatching lengths!")

        existing_tables = self.engine.get_all_tables(target_repository.to_schema())
        clashing = [t for t in target_tables if t in existing_tables]
        if clashing:
            raise ValueError("Table(s) %r already exist(s) at %s!" % (clashing, target_repository))

        return self._import_tables(image_hash, tables, target_repository, target_hash, target_tables, do_checkout,
                                   table_queries, foreign_tables)

    def _import_tables(self, image_hash, tables, target_repository, target_hash, target_tables, do_checkout,
                       table_queries, foreign_tables):
        engine = self.engine
        engine.create_schema(target_repository.to_schema())

        # This importing route only supported between local repos.
        assert engine == target_repository.engine

        head = target_repository.get_head(raise_on_none=False)
        # Add the new snap ID to the tree
        add_new_image(target_repository, head, target_hash, comment="Importing %s from %s" % (tables, self))

        if any(table_queries) and not foreign_tables:
            # If we want to run some queries against the source repository to create the new tables,
            # we have to materialize it fully.
            self.checkout(image_hash)
        # Materialize the actual tables in the target repository and register them.
        image = self.get_image(image_hash)

        for table, target_table, is_query in zip(tables, target_tables, table_queries):
            if foreign_tables or is_query:
                # For foreign tables/SELECT queries, we define a new object/table instead.
                object_id = get_random_object_id()
                if is_query:
                    # is_query precedes foreign_tables: if we're importing using a query, we don't care if it's a
                    # foreign table or not since we're storing it as a full snapshot.
                    engine.execute_sql_in(self.to_schema(),
                                          SQL("CREATE TABLE {}.{} AS ").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                               Identifier(object_id)) + SQL(table))
                elif foreign_tables:
                    self.engine.copy_table(self.to_schema(), table, SPLITGRAPH_META_SCHEMA, object_id)

                # Might not be necessary if we don't actually want to materialize the snapshot (wastes space).
                self.objects.register_object(object_id, 'SNAP', namespace=target_repository.namespace,
                                             parent_object=None)
                self.objects.register_table(target_repository, target_table, target_hash, object_id)
                if do_checkout:
                    self.engine.copy_table(SPLITGRAPH_META_SCHEMA, object_id, target_repository.to_schema(),
                                           target_table)
            else:
                table_obj = image.get_table(table)
                for object_id, _ in table_obj.objects:
                    self.objects.register_table(target_repository, target_table, target_hash, object_id)
                if do_checkout:
                    table_obj.materialize(target_table, destination_schema=target_repository.to_schema())
        # Register the existing tables at the new commit as well.
        if head is not None:
            # Maybe push this into the tables API (currently have to make 2 queries)
            engine.run_sql(SQL("""INSERT INTO {0}.tables (namespace, repository, image_hash, table_name, object_id)
                    (SELECT %s, %s, %s, table_name, object_id FROM {0}.tables
                    WHERE namespace = %s AND repository = %s AND image_hash = %s)""")
                           .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                           (target_repository.namespace, target_repository.repository,
                            target_hash, target_repository.namespace, target_repository.repository, head),
                           return_shape=None)
        set_head(target_repository, target_hash)
        return target_hash

    def get_image(self, image_hash):
        """
        Gets information about a single image.

        :param image_hash: Image hash
        :return: An Image object or None
        """
        result = self.engine.run_sql(select("images", ','.join(IMAGE_COLS),
                                            "repository = %s AND image_hash = %s AND namespace = %s"),
                                     (self.repository,
                                      image_hash, self.namespace),
                                     return_shape=ResultShape.ONE_MANY)
        if not result:
            return None
        r_dict = {k: v for k, v in zip(IMAGE_COLS, result)}
        r_dict.update(repository=self)
        return Image(**r_dict)

    def get_images(self):
        """
        Get all images in the repository

        :return: List of Image objects
        """
        result = []
        for image in self.engine.run_sql(select("images", ','.join(IMAGE_COLS), "repository = %s AND namespace = %s") +
                                         SQL(" ORDER BY created"), (self.repository, self.namespace)):
            r_dict = {k: v for k, v in zip(IMAGE_COLS, image)}
            r_dict.update(repository=self)
            result.append(Image(**r_dict))
        return result

    def push(self, remote_repository=None, handler='DB', handler_options=None):
        """
        Inverse of ``pull``: Pushes all local changes to the remote and uploads new objects.
.
        :param remote_repository: Remote repository to push changes to. If not specified, the current
            upstream is used.
        :param handler: Name of the handler to use to upload objects. Use `DB` to push them to the remote or `S3`
            to store them in an S3 bucket.
        :param handler_options: Extra options to pass to the handler. For example, see
            :class:`splitgraph.hooks.s3.S3ExternalObjectHandler`
        """

        if handler_options is None:
            handler_options = {}
        ensure_metadata_schema(self.engine)

        # Maybe consider having a context manager for getting a remote engine instance
        # that auto-commits/closes when needed?
        remote_repository = remote_repository or self.get_upstream()
        if not remote_repository:
            raise SplitGraphException("No remote repository specified and no upstream found for %s!" % self.to_schema())

        try:
            logging.info("Gathering remote metadata...")
            # Flip the two connections here: pretend the remote engine is local and download metadata from the local
            # engine instead of the remote.
            # This also registers new commits remotely. Should make explicit and move down later on.
            new_images, table_meta, object_locations, object_meta, tags = \
                _get_required_snaps_objects(remote_repository, self)

            if not new_images:
                logging.info("Nothing to do.")
                return

            new_uploads = self.objects.upload_objects(remote_repository.objects, list(set(o[0] for o in object_meta)),
                                                      handler=handler, handler_params=handler_options)
            # Register the newly uploaded object locations locally and remotely.
            remote_repository.objects.register_objects(object_meta)
            remote_repository.objects.register_object_locations(object_locations + new_uploads)
            remote_repository.objects.register_tables(remote_repository, table_meta)
            remote_repository.set_tags(tags, force=False)

            self.objects.register_object_locations(new_uploads)

            if self.get_upstream() is None:
                self.set_upstream(remote_repository)

            remote_repository.engine.commit()
            print("Uploaded metadata for %d object(s), %d table version(s) and %d tag(s)." % (len(object_meta),
                                                                                              len(table_meta),
                                                                                              len([t for t in tags if
                                                                                                   t != 'HEAD'])))
        finally:
            remote_repository.engine.close()

    def pull(self, download_all=False):
        """
        Synchronizes the state of the local Splitgraph repository with its upstream, optionally downloading all new
        objects created on the remote.

        :param download_all: If True, downloads all objects and stores them locally. Otherwise, will only download required
            objects when a table is checked out.
        """
        upstream = self.get_upstream()
        if not upstream:
            raise SplitGraphException("No upstream found for repository %s!" % self.to_schema())

        clone(remote_repository=upstream, local_repository=self, download_all=download_all)

    def has_pending_changes(self):
        """
        Detects if the repository has any pending changes (schema changes, table additions/deletions, content changes).
        """
        head = self.get_head(raise_on_none=False)
        if not head:
            # If the repo isn't checked out, no point checking for changes.
            return False
        for table in self.engine.get_all_tables(self.to_schema()):
            if self.diff(table, head, None, aggregate=True) != (0, 0, 0):
                return True
        return False

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

        image_hash = self.get_tagged_id(tag)
        logging.info("Publishing %s:%s (%s)", self, image_hash, tag)

        image = self.get_image(image_hash)
        dependencies = [((r.namespace, r.repository), i) for r, i in image.provenance()] \
            if include_provenance else None
        previews, schemata = _prepare_extra_data(image, self, include_table_previews)

        try:
            publish_tag(remote_repository, tag, image_hash, datetime.now(), dependencies, readme, schemata=schemata,
                        previews=previews if include_table_previews else None)
            remote_repository.engine.commit()
        finally:
            remote_repository.engine.close()

    def get_head(self, raise_on_none=True):
        """
        Gets the currently checked out image hash

        :param raise_on_none: Whether to raise an error or return None if the repository isn't checked out.
        """
        return self.get_tagged_id('HEAD', raise_on_none)

    def run_sql(self, sql):
        self.engine.run_sql("SET search_path TO %s", (self.to_schema(),))
        result = self.engine.run_sql(sql)
        self.engine.run_sql("SET search_path TO public")
        return result

    def diff(self, table_name, image_1, image_2, aggregate=False):
        """
        Compares the state of a table in different images. If the two images are on the same path in the commit tree,
        it doesn't need to materialize any of the tables and simply aggregates their DIFF objects to produce a complete
        changelog. Otherwise, it materializes both tables into a temporary space and compares them row-to-row.

        :param table_name: Name of the table.
        :param image_1: First image. If None, uses the state of the current staging area.
        :param image_2: Second image. If None, uses the state of the current staging area.
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
                * `action_type == 2`: Update, `action_data` is a dictionary of non-PK columns and their new values for that
                  particular row.
        """

        # If the table doesn't exist in the first or the second image, short-circuit and
        # return the bool.
        if not table_exists_at(self, table_name, image_1):
            return True
        if not table_exists_at(self, table_name, image_2):
            return False

        # Special case: if diffing HEAD and staging, then just return the current pending changes.
        head = self.get_head()

        if image_1 == head and image_2 is None:
            changes = self.engine.get_pending_changes(self.to_schema(), table_name, aggregate=aggregate)
            return list(changes) if not aggregate else _changes_to_aggregation(changes)

        # If the table is the same in the two images, short circuit as well.
        if image_2 is not None:
            if set(self.get_image(image_1).get_table(table_name).objects) == \
                    set(self.get_image(image_2).get_table(table_name).objects):
                return [] if not aggregate else (0, 0, 0)

        # Otherwise, check if image_1 is a parent of image_2, then we can merge all the diffs.
        # FIXME: we have to find if there's a path between two _objects_ representing these tables that's made out
        # of DIFFs.
        path = find_path(self, image_1, (image_2 if image_2 is not None else head))
        if path is not None:
            result = _calculate_merged_diff(self, table_name, path, aggregate)
            if result is None:
                return _side_by_side_diff(self, table_name, image_1, image_2, aggregate)

            # If snap_2 is staging, also include all changes that have happened since the last commit.
            if image_2 is None:
                changes = self.engine.get_pending_changes(self.to_schema(), table_name, aggregate=aggregate)
                if aggregate:
                    return _changes_to_aggregation(changes, result)
                result.extend(changes)
            return result

        # Finally, resort to manual diffing (images on different branches or reverse comparison order).
        return _side_by_side_diff(self, table_name, image_1, image_2, aggregate)


def to_repository(schema):
    """Converts a Postgres schema name of the format `namespace/repository` to a Splitgraph repository object."""
    if '/' in schema:
        namespace, repository = schema.split('/')
        return Repository(namespace, repository)
    return Repository('', schema)


def _changes_to_aggregation(query_result, initial=None):
    result = list(initial) if initial else [0, 0, 0]
    for kind, kind_count in query_result:
        result[kind] += kind_count
    return tuple(result)


def _calculate_merged_diff(repository, table_name, path, aggregate):
    result = [] if not aggregate else (0, 0, 0)
    for image in reversed(path):
        diff_id = repository.get_image(image).get_table(table_name).get_object('DIFF')
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
            result = _changes_to_aggregation(
                repository.engine.run_sql(
                    SQL("SELECT sg_action_kind, count(sg_action_kind) FROM {}.{} GROUP BY sg_action_kind")
                        .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(diff_id))), result)
    return result


def _side_by_side_diff(repository, table_name, image_1, image_2, aggregate):
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
    return [(r, 1, None) for r in left if r not in right] + \
           [(r, 0, {'c': [], 'v': []}) for r in right if r not in left]


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
    tmp_mountpoint.import_tables(remote_tables, target_repository, target_tables, image_hash=remote_image_hash,
                                 target_hash=target_hash)

    tmp_mountpoint.rm()
    target_repository.engine.commit()


def find_path(repository, hash_1, hash_2):
    """If the two images are on the same path in the commit tree, returns that path."""
    path = []
    while hash_2 is not None:
        path.append(hash_2)
        hash_2 = repository.get_image(hash_2).parent_id
        if hash_2 == hash_1:
            return path


def table_exists_at(repository, table_name, image_hash):
    """Determines whether a given table exists in a Splitgraph image without checking it out. If `image_hash` is None,
    determines whether the table exists in the current staging area."""
    return repository.engine.table_exists(repository.to_schema(), table_name) if image_hash is None \
        else bool(repository.get_image(image_hash).get_table(table_name))


def _prepare_extra_data(image, repository, include_table_previews):
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
            schema = image.get_table(table_name).get_schema()
        schemata[table_name] = [(cn, ct, pk) for _, cn, ct, pk in schema]
    return previews, schemata


def _get_required_snaps_objects(target, source):
    """
    Inspects two Splitgraph repositories and gathers metadata that is required to bring target up to
    date with source.

    :param target: Target Repository object
    :param source: Source repository object
    """

    target_images = {i.image_hash: i for i in target.get_images()}
    source_images = {i.image_hash: i for i in source.get_images()}

    # We assume here that none of the target image hashes have changed (are immutable) since otherwise the target
    # would have created a new images
    table_meta = []
    new_images = [i for i in source_images if i not in target_images]
    for image_hash in new_images:
        image = source_images[image_hash]
        # This is not batched but there shouldn't be that many entries here anyway.
        add_new_image(target, image.parent_id, image.image_hash, image.created, image.comment, image.provenance_type,
                      image.provenance_data)
        # Get the meta for all objects we'll need to fetch.
        table_meta.extend(source.engine.run_sql(
            SQL("""SELECT image_hash, table_name, object_id FROM {0}.tables
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
    """
    if isinstance(remote_repository, str):
        remote_repository = lookup_repo(remote_repository, include_local=False)

    # Repository engine should be local by default
    if not local_repository:
        local_repository = Repository(remote_repository.namespace, remote_repository.repository)

    # Get the remote log and the list of objects we need to fetch.
    logging.info("Gathering remote metadata...")

    # This also registers the new versions locally.
    new_images, table_meta, object_locations, object_meta, tags = _get_required_snaps_objects(local_repository,
                                                                                              remote_repository)

    if not new_images:
        logging.info("Nothing to do.")
        return

    # Don't actually download any real objects until the user tries to check out a revision.
    if download_all:
        # Check which new objects we need to fetch/preregister.
        # We might already have some objects prefetched
        # (e.g. if a new version of the table is the same as the old version)
        logging.info("Fetching remote objects...")
        local_repository.objects.download_objects(
            remote_repository.objects, objects_to_fetch=list(set(o[0] for o in object_meta)),
            object_locations=object_locations)

    # Map the tables to the actual objects no matter whether or not we're downloading them.
    local_repository.objects.register_objects(object_meta)
    local_repository.objects.register_object_locations(object_locations)
    local_repository.objects.register_tables(local_repository, table_meta)
    local_repository.set_tags(tags, force=False)
    # Don't check anything out, keep the repo bare.
    set_head(local_repository, None)

    print("Fetched metadata for %d object(s), %d table version(s) and %d tag(s)." % (len(object_meta),
                                                                                     len(table_meta),
                                                                                     len([t for t in tags if
                                                                                          t != 'HEAD'])))

    if local_repository.get_upstream() is None:
        local_repository.set_upstream(remote_repository)
