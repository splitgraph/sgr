import logging
from contextlib import contextmanager
from random import getrandbits

from psycopg2.sql import SQL, Identifier

import splitgraph as sg
from splitgraph import get_engine, SplitGraphException, rm, delete_tag, get_current_head, clone
from splitgraph._data.common import ensure_metadata_schema, select
from splitgraph._data.images import add_new_image, get_image_object_path, IMAGE_COLS
from splitgraph._data.objects import register_table, get_external_object_locations, get_existing_objects, \
    get_object_for_table, register_object
from splitgraph.commands._common import manage_audit_triggers, set_head, manage_audit
from splitgraph.commands.misc import delete_objects, table_exists_at, find_path
from splitgraph.commands.tagging import get_tagged_id
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.engine import ResultShape
from ._objects.creation import record_table_as_snap, record_table_as_diff
from ._objects.loading import download_objects
from ._objects.utils import get_random_object_id
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
    Splitgraph repository API
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
            image_hash = self.get_canonical_image_id(image_hash)
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
                snap_1 = get_image_object_path(self, table, current_head)[0]
                if get_engine().get_full_table_schema(SPLITGRAPH_META_SCHEMA, snap_1) != \
                        get_engine().get_full_table_schema(self.to_schema(), table):
                    record_table_as_snap(table_info, image_hash)
                    continue

                if table in changed_tables:
                    record_table_as_diff(table_info, image_hash)
                else:
                    # If the table wasn't changed, point the commit to the old table objects (including
                    # any of snaps or diffs).
                    # This feels slightly weird: are we denormalized here?
                    for prev_object_id, _ in table_info.objects:
                        register_table(self, table, image_hash, prev_object_id)

            # If table created (or we want to store a snap anyway), copy the whole table over as well.
            if not table_info or include_snap:
                record_table_as_snap(table_info, image_hash, table_name=table, repository=self)

        # Make sure that all pending changes have been discarded by this point (e.g. if we created just a snapshot for
        # some tables and didn't consume the audit log).
        # NB if we allow partial commits, this will have to be changed (only discard for committed tables).
        get_engine().discard_pending_changes(target_schema)

    get_all_hashes_tags = sg.get_all_hashes_tags

    def get_canonical_image_id(self, short_image):
        """
        Converts a truncated image hash into a full hash. Raises if there's an ambiguity.

        :param short_image: Shortened image hash
        """
        candidates = get_engine().run_sql(select("images", "image_hash",
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

    resolve_image = sg.resolve_image

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
            image_hash = image_hash or get_current_head(self)

        if not tables:
            tables = self.get_image(image_hash).get_tables() if not foreign_tables \
                else get_engine().get_all_tables(self.to_schema())
        if not target_tables:
            if table_queries:
                raise ValueError("target_tables has to be defined if table_queries is True!")
            target_tables = tables
        if not table_queries:
            table_queries = [False] * len(tables)
        if len(tables) != len(target_tables) or len(tables) != len(table_queries):
            raise ValueError("tables, target_tables and table_queries have mismatching lengths!")

        existing_tables = get_engine().get_all_tables(target_repository.to_schema())
        clashing = [t for t in target_tables if t in existing_tables]
        if clashing:
            raise ValueError("Table(s) %r already exist(s) at %s!" % (clashing, target_repository))

        return self._import_tables(image_hash, tables, target_repository, target_hash, target_tables, do_checkout,
                                   table_queries, foreign_tables)

    def _import_tables(self, image_hash, tables, target_repository, target_hash, target_tables, do_checkout,
                       table_queries, foreign_tables):
        engine = get_engine()
        engine.create_schema(target_repository.to_schema())

        head = get_current_head(target_repository, raise_on_none=False)
        # Add the new snap ID to the tree
        add_new_image(target_repository, head, target_hash, comment="Importing %s from %s" % (tables, self))

        if any(table_queries) and not foreign_tables:
            # If we want to run some queries against the source repository to create the new tables,
            # we have to materialize it fully.
            self.checkout(image_hash)
        # Materialize the actual tables in the target repository and register them.
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
                    get_engine().copy_table(self.to_schema(), table, SPLITGRAPH_META_SCHEMA, object_id)

                _register_and_checkout_new_table(do_checkout, object_id, target_hash, target_repository, target_table)
            else:
                for object_id, _ in self.get_image(image_hash).get_table(table).objects:
                    register_table(target_repository, target_table, target_hash, object_id)
                if do_checkout:
                    self.materialize_table(image_hash, table, target_table,
                                           destination_schema=target_repository.to_schema())
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

    def has_pending_changes(self):
        """
        Detects if the repository has any pending changes (schema changes, table additions/deletions, content changes).

        :param self: Repository object
        """
        head = get_current_head(self, raise_on_none=False)
        if not head:
            # If the repo isn't checked out, no point checking for changes.
            return False
        for table in get_engine().get_all_tables(self.to_schema()):
            if self.diff(table, head, None, aggregate=True) != (0, 0, 0):
                return True
        return False

    publish = sg.publish

    def get_head(self, raise_on_none=True):
        return sg.get_current_head(self, raise_on_none=raise_on_none)

    def run_sql(self, sql):
        engine = self.engine
        engine.run_sql("SET search_path TO %s", (self.to_schema(),))
        result = self.engine.run_sql(sql)
        engine.run_sql("SET search_path TO public")
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
        head = get_current_head(self)

        if image_1 == head and image_2 is None:
            changes = get_engine().get_pending_changes(self.to_schema(), table_name, aggregate=aggregate)
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
                changes = get_engine().get_pending_changes(self.to_schema(), table_name, aggregate=aggregate)
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
        diff_id = get_object_for_table(repository, table_name, image, 'DIFF')
        if diff_id is None:
            # There's a SNAP entry between the two images, meaning there has been a schema change.
            # Hence we can't accumulate the DIFFs and have to resort to manual side-by-side diffing.
            return None
        if not aggregate:
            # There's only one action applied to a tuple in a single diff, so the ordering doesn't matter.
            for row in sorted(
                    get_engine().run_sql(SQL("SELECT * FROM {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                           Identifier(diff_id)))):
                pk = row[:-2]
                action = row[-2]
                action_data = row[-1]
                result.append((pk, action, action_data))
        else:
            result = _changes_to_aggregation(
                get_engine().run_sql(
                    SQL("SELECT sg_action_kind, count(sg_action_kind) FROM {}.{} GROUP BY sg_action_kind")
                        .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(diff_id))), result)
    return result


def _side_by_side_diff(repository, table_name, image_1, image_2, aggregate):
    with repository.materialized_table(table_name, image_1) as (mp_1, table_1):
        with repository.materialized_table(table_name, image_2) as (mp_2, table_2):
            # Check both tables out at the same time since then table_2 calculation can be based
            # on table_1's snapshot.
            left = get_engine().run_sql(SQL("SELECT * FROM {}.{}").format(Identifier(mp_1),
                                                                          Identifier(table_1)))
            right = get_engine().run_sql(SQL("SELECT * FROM {}.{}").format(Identifier(mp_2),
                                                                           Identifier(table_2)))

    if aggregate:
        return sum(1 for r in right if r not in left), sum(1 for r in left if r not in right), 0
    # Mimic the diff format returned by the DIFF-object-accumulating function
    return [(r, 1, None) for r in left if r not in right] + \
           [(r, 0, {'c': [], 'v': []}) for r in right if r not in left]


def _register_and_checkout_new_table(do_checkout, object_id, target_hash, target_repository, target_table):
    # Might not be necessary if we don't actually want to materialize the snapshot (wastes space).
    register_object(object_id, 'SNAP', namespace=target_repository.namespace, parent_object=None)
    register_table(target_repository, target_table, target_hash, object_id)
    if do_checkout:
        get_engine().copy_table(SPLITGRAPH_META_SCHEMA, object_id, target_repository.to_schema(), target_table)


def import_table_from_remote(remote_engine, remote_repository, remote_tables, remote_image_hash, target_repository,
                             target_tables, target_hash=None):
    """
    Shorthand for importing one or more tables from a yet-uncloned remote. Here, the remote image hash is required,
    as otherwise we aren't necessarily able to determine what the remote head is.

    :param remote_engine: Remote engine name
    :param remote_repository: Remote repository
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

    clone(remote_repository, remote_engine=remote_engine, local_repository=tmp_mountpoint, download_all=False)
    tmp_mountpoint.import_tables(remote_tables, target_repository, target_tables, image_hash=remote_image_hash,
                                 target_hash=target_hash)

    rm(tmp_mountpoint)
    get_engine().commit()
