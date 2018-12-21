import logging
from contextlib import contextmanager
from random import getrandbits

from psycopg2.sql import SQL, Identifier

import splitgraph as sg
from splitgraph import get_engine, SplitGraphException, rm, delete_tag
from splitgraph._data.common import ensure_metadata_schema, select
from splitgraph._data.images import add_new_image, get_image_object_path, IMAGE_COLS
from splitgraph._data.objects import register_table, get_external_object_locations, get_existing_objects, \
    get_object_for_table
from splitgraph.commands._common import manage_audit_triggers, set_head, manage_audit
from splitgraph.commands._objects.creation import record_table_as_snap, record_table_as_diff
from splitgraph.commands._objects.loading import download_objects
from splitgraph.commands._objects.utils import get_random_object_id
from splitgraph.commands.diff import has_pending_changes as _has_changes
from splitgraph.commands.info import table_schema_changed, get_canonical_image_id
from splitgraph.commands.misc import delete_objects
from splitgraph.commands.tagging import get_tagged_id
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.engine import ResultShape
from .image import Image


# OO API porting plan:
# [x] make shims, direct them to splitgraph.commands
# [x] direct commandline, tests and splitfile interpreter to the shims
# [ ] move the commands inside the shims
# [ ] review current internal metadata access/creation commands to see which can be made
#     into private methods
# [ ] make sure Repo objects use their own internal engine pointer
# [ ] remove switch_engine -- instead use fixtures like PG_MNT_REMOTE/PG_MNT


class Repository:
    """
    Shim, experimenting with Splitgraph OO API

    Currently calls into the real API functions but we'll obviously move them here if we go ahead with this.
    """

    def __init__(self, namespace, repository, engine=None):
        self.namespace = namespace
        self.repository = repository

        self.engine = engine or sg.get_engine()

    def __eq__(self, other):
        return self.namespace == other.namespace and self.repository == other.repository \
               and self.engine == other.engine

    def to_schema(self):
        return self.namespace + "/" + self.repository if self.namespace else self.repository

    __str__ = to_schema
    __repr__ = to_schema

    def __hash__(self):
        return hash(self.namespace) * hash(self.repository)

    def get_upstream(self):
        """
        Gets the current upstream (connection string and repository) that a local repository tracks

        :param self: Local repository
        :return: Tuple of (remote engine, remote Repository object)
        """
        result = get_engine().run_sql(select("upstream", "remote_name, remote_namespace, remote_repository",
                                             "namespace = %s AND repository = %s"),
                                      (self.namespace, self.repository),
                                      return_shape=ResultShape.ONE_MANY)
        if result is None:
            return result
        return result[0], Repository(result[1], result[2])

    def set_upstream(self, remote_name, remote_repository):
        """
        Sets the upstream remote + repository that this repository tracks.

        :param self: Local repository
        :param remote_name: Name of the remote as specified in the Splitgraph config.
        :param remote_repository: Remote Repository object
        """
        get_engine().run_sql(SQL("INSERT INTO {0}.upstream (namespace, repository, "
                                 "remote_name, remote_namespace, remote_repository) VALUES (%s, %s, %s, %s, %s)"
                                 " ON CONFLICT (namespace, repository) DO UPDATE SET "
                                 "remote_name = excluded.remote_name, remote_namespace = excluded.remote_namespace, "
                                 "remote_repository = excluded.remote_repository WHERE "
                                 "upstream.namespace = excluded.namespace "
                                 "AND upstream.repository = excluded.repository")
                             .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                             (self.namespace, self.repository, remote_name, remote_repository.namespace,
                              remote_repository.repository))

    def delete_upstream(self):
        """
        Deletes the upstream remote + repository for a local repository.

        :param self: Local repository
        """
        get_engine().run_sql(SQL("DELETE FROM {0}.upstream WHERE namespace = %s AND repository = %s")
                             .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                             (self.namespace, self.repository),
                             return_shape=None)

    def materialize_table(self, image_hash, table, destination, destination_schema=None):
        """
        Materializes a Splitgraph table in the target schema as a normal Postgres table, potentially downloading all
        required objects and using them to reconstruct the table.

        :param image_hash: Hash of the commit to get the table from.
        :param table: Name of the table.
        :param destination: Name of the destination table.
        :param destination_schema: Name of the destination schema.
        :return: A set of IDs of downloaded objects used to construct the table.
        """
        destination_schema = destination_schema or self.to_schema()
        engine = get_engine()
        engine.delete_table(destination_schema, destination)
        # Get the closest snapshot from the table's parents
        # and then apply all deltas consecutively from it.
        object_id, to_apply = get_image_object_path(self, table, image_hash)

        # Make sure all the objects have been downloaded from remote if it exists
        remote_info = self.get_upstream()
        if remote_info:
            object_locations = get_external_object_locations(to_apply + [object_id])
            fetched_objects = download_objects(remote_info[0],
                                               objects_to_fetch=to_apply + [object_id],
                                               object_locations=object_locations)

        difference = set(to_apply + [object_id]).difference(set(get_existing_objects()))
        if difference:
            logging.warning("Not all objects required to materialize %s:%s:%s exist locally.",
                            self.to_schema(), image_hash, table)
            logging.warning("Missing objects: %r", difference)

        # Copy the given snap id over to "staging" and apply the DIFFS
        engine.copy_table(SPLITGRAPH_META_SCHEMA, object_id, destination_schema, destination,
                          with_pk_constraints=True)
        for pack_object in reversed(to_apply):
            logging.info("Applying %s...", pack_object)
            engine.apply_diff_object(SPLITGRAPH_META_SCHEMA, pack_object, destination_schema, destination)

        return fetched_objects if remote_info else set()

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
        object_id = get_object_for_table(self, table_name, image_hash, 'SNAP')
        if object_id is None:
            # Materialize the SNAP into a new object
            new_id = get_random_object_id()
            self.materialize_table(image_hash, table_name, new_id, destination_schema=SPLITGRAPH_META_SCHEMA)
            yield SPLITGRAPH_META_SCHEMA, new_id
            # Maybe some cache management/expiry strategies here
            delete_objects([new_id])
        else:
            if get_engine().table_exists(SPLITGRAPH_META_SCHEMA, object_id):
                yield SPLITGRAPH_META_SCHEMA, object_id
            else:
                # The SNAP object doesn't actually exist remotely, so we have to download it.
                # An optimisation here: we could open an RO connection to the remote instead if the object
                # does live there.
                remote_info = self.get_upstream()
                if not remote_info:
                    raise SplitGraphException("SNAP %s from %s doesn't exist locally and no remote was found for it!"
                                              % (object_id, str(self)))
                remote_conn, _ = remote_info
                object_locations = get_external_object_locations([object_id])
                download_objects(remote_conn, objects_to_fetch=[object_id], object_locations=object_locations)
                yield SPLITGRAPH_META_SCHEMA, object_id

                delete_objects([object_id])

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
        engine = get_engine()
        if tables is None:
            tables = []
        if self.has_pending_changes():
            if not force:
                raise SplitGraphException("{0} has pending changes! Pass force=True or do sgr checkout -f {0}:HEAD"
                                          .format(self.to_schema()))
            logging.warning("%s has pending changes, discarding...", self.to_schema())
            engine.discard_pending_changes(target_schema)
        # Detect the actual image
        if image_hash:
            # get_canonical_image_hash called twice if the commandline entry point already called it. How to fix?
            image_hash = get_canonical_image_id(self, image_hash)
        elif tag:
            image_hash = get_tagged_id(self, tag)
        else:
            raise SplitGraphException("One of image_hash or tag must be specified!")

        tables = tables or self.get_image(image_hash).get_tables()
        # Drop all current tables in staging
        for table in engine.get_all_tables(target_schema):
            engine.delete_table(target_schema, table)

        downloaded_object_ids = set()
        for table in tables:
            downloaded_object_ids |= self.materialize_table(image_hash, table, table)

        # Repoint the current HEAD for this mountpoint to the new snap ID
        set_head(self, image_hash)

        if not keep_downloaded_objects:
            logging.info("Removing %d downloaded objects from cache...", downloaded_object_ids)
            delete_objects(downloaded_object_ids)

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
        rm(self, unregister=False)
        delete_tag(self, 'HEAD')

    def commit(self, image_hash=None, include_snap=False, comment=None):
        """
        Commits all pending changes to a given repository, creating a new image.

        :param self: Repository to commit.
        :param image_hash: Hash of the commit. Chosen by random if unspecified.
        :param include_snap: If True, also creates a SNAP object with a full copy of the table. This will speed up
            checkouts, but consumes extra space.
        :param comment: Optional comment to add to the commit.
        :return: The image hash the current state of the mountpoint was committed under.
        """
        target_schema = self.to_schema()

        ensure_metadata_schema()
        get_engine().commit()
        manage_audit_triggers()

        logging.info("Committing %s...", target_schema)

        head = self.get_head()

        if image_hash is None:
            image_hash = "%0.2x" % getrandbits(256)

        # Add the new snap ID to the tree
        add_new_image(self, head, image_hash, comment=comment)

        self._commit(head, image_hash, include_snap=include_snap)

        set_head(self, image_hash)
        get_engine().commit()
        manage_audit_triggers()
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
        engine = get_engine()

        head = self.get_image(current_head)

        changed_tables = engine.get_changed_tables(target_schema)
        for table in engine.get_all_tables(target_schema):
            table_info = head.get_table(table)
            # Table already exists at the current HEAD
            if table_info:
                # If there has been a schema change, we currently just snapshot the whole table.
                # This is obviously wasteful (say if just one column has been added/dropped or we added a PK,
                # but it's a starting point to support schema changes.
                if table_schema_changed(self, table, image_1=current_head, image_2=None):
                    record_table_as_snap(self, image_hash, table, table_info.objects)
                    continue

                if table in changed_tables:
                    record_table_as_diff(self, image_hash, table, table_info.objects)
                else:
                    # If the table wasn't changed, point the commit to the old table objects (including
                    # any of snaps or diffs).
                    # This feels slightly weird: are we denormalized here?
                    for prev_object_id, _ in table_info.objects:
                        register_table(self, table, image_hash, prev_object_id)

            # If table created (or we want to store a snap anyway), copy the whole table over as well.
            if not table_info or include_snap:
                record_table_as_snap(self, image_hash, table, table_info.objects if table_info else [])

        # Make sure that all pending changes have been discarded by this point (e.g. if we created just a snapshot for
        # some tables and didn't consume the audit log).
        # NB if we allow partial commits, this will have to be changed (only discard for committed tables).
        get_engine().discard_pending_changes(target_schema)

    get_all_hashes_tags = sg.get_all_hashes_tags
    resolve_image = sg.resolve_image
    import_tables = sg.import_tables

    def get_image(self, image_hash):
        """
        Gets information about a single image.

        :param image_hash: Image hash
        :return: An Image object or None
        """
        result = get_engine().run_sql(select("images", ','.join(IMAGE_COLS),
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
        return [img[0] for img in sg.get_all_image_info(self)]

    push = sg.push
    pull = sg.pull
    has_pending_changes = _has_changes
    publish = sg.publish

    def get_head(self, raise_on_none=True):
        return sg.get_current_head(self, raise_on_none=raise_on_none)

    def run_sql(self, sql):
        engine = self.engine
        engine.run_sql("SET search_path TO %s", (self.to_schema(),))
        result = self.engine.run_sql(sql)
        engine.run_sql("SET search_path TO public")
        return result

    def diff(self, table_name, image_1, image_2=None, aggregate=False):
        return sg.diff(self, table_name, image_1, image_2, aggregate)


def to_repository(schema):
    """Converts a Postgres schema name of the format `namespace/repository` to a Splitgraph repository object."""
    if '/' in schema:
        namespace, repository = schema.split('/')
        return Repository(namespace, repository)
    return Repository('', schema)
