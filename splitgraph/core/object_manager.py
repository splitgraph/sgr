"""Functions related to creating, deleting and keeping track of physical Splitgraph objects."""
import itertools
import logging
from collections import defaultdict
from contextlib import contextmanager
from random import getrandbits

from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.engine import ResultShape, switch_engine
from splitgraph.exceptions import SplitGraphException
from splitgraph.hooks.external_objects import get_external_object_handler
from ._common import META_TABLES, select, insert, pretty_size


class ObjectManager:
    """A Splitgraph metadata-aware class that keeps track of objects on a given engine.
    Backed by ObjectEngine to move physical objects around and run metadata queries."""

    def __init__(self, object_engine):
        """
        :param object_engine: An ObjectEngine that will be used as a backing store for the
            objects.
        """
        self.object_engine = object_engine

        # Cache size in bytes
        self.cache_size = 1024 * 1024 * 1024

    def get_full_object_tree(self):
        """Returns a dictionary (object_id -> [list of parents], SNAP/DIFF, size) with the full object tree
        in the engine"""
        query_result = self.object_engine.run_sql(select("objects", "object_id,parent_id,format,size"))

        result = {}
        for object_id, parent_id, object_format, size in query_result:
            if object_id not in result:
                result[object_id] = ([parent_id], object_format, size) if parent_id else ([], object_format, size)
            else:
                if parent_id:
                    result[object_id][0].append(parent_id)

        return result

    def register_object(self, object_id, object_format, namespace, parent_object=None):
        """
        Registers a Splitgraph object in the object tree

        :param object_id: Object ID
        :param object_format: Format (SNAP or DIFF)
        :param namespace: Namespace that owns the object. In registry mode, only namespace owners can alter or delete
            objects.
        :param parent_object: Parent that the object depends on, if it's a DIFF object.
        """
        if not parent_object and object_format != 'SNAP':
            raise ValueError("Non-SNAP objects can't have no parent!")
        if parent_object and object_format == 'SNAP':
            raise ValueError("SNAP objects can't have a parent!")

        object_size = self.object_engine.get_table_size(SPLITGRAPH_META_SCHEMA, object_id)
        self.object_engine.run_sql(insert("objects", ("object_id", "format", "parent_id", "namespace", "size")),
                                   (object_id, object_format, parent_object, namespace, object_size))
        # Also add a cache status entry for the object.
        if not self.object_engine.run_sql(select("object_cache_status", "1", "object_id=%s"), (object_id,)):
            self.object_engine.run_sql(insert("object_cache_status", ("object_id", "status", "refcount")),
                                       (object_id, 2, 0))

    def register_objects(self, object_meta, namespace=None):
        """
        Registers multiple Splitgraph objects in the tree. See `register_object` for more information.

        :param object_meta: List of (object_id, format, parent_id, namespace, size).
        :param namespace: If specified, overrides the original object namespace, required
            in the case where the remote repository has a different namespace than the local one.
        """
        if namespace:
            object_meta = [(o, f, p, namespace, s) for o, f, p, _, s in object_meta]
        self.object_engine.run_sql_batch(insert("objects", ("object_id", "format", "parent_id", "namespace", "size")),
                                         object_meta)
        distinct_objects = set(o[0] for o in object_meta)
        self.object_engine.run_sql_batch(insert("object_cache_status", ("object_id", "status", "refcount")),
                                         [(o, 2, 0) for o in distinct_objects])

    def register_tables(self, repository, table_meta):
        """
        Links tables in an image to physical objects that they are stored as.
        Objects must already be registered in the object tree.

        :param repository: Repository that the tables belong to.
        :param table_meta: A list of (image_hash, table_name, table_schema, object_id).
        """
        table_meta = [(repository.namespace, repository.repository,
                       o[0], o[1], Json(o[2]), o[3]) for o in table_meta]
        self.object_engine.run_sql_batch(
            insert("tables", ("namespace", "repository", "image_hash", "table_name", "table_schema", "object_id")),
            table_meta)

    def register_object_locations(self, object_locations):
        """
        Registers external locations (e.g. HTTP or S3) for Splitgraph objects.
        Objects must already be registered in the object tree.

        :param object_locations: List of (object_id, location, protocol).
        """
        # Don't insert redundant objects here either.
        existing_locations = self.object_engine.run_sql(select("object_locations", "object_id"),
                                                        return_shape=ResultShape.MANY_ONE)
        object_locations = [o for o in object_locations if o[0] not in existing_locations]
        self.object_engine.run_sql_batch(insert("object_locations", ("object_id", "location", "protocol")),
                                         object_locations)

    def get_existing_objects(self):
        """
        Gets all objects currently in the Splitgraph tree.

        :return: Set of object IDs.
        """
        return set(self.object_engine.run_sql(select("objects", "object_id"), return_shape=ResultShape.MANY_ONE))

    def get_downloaded_objects(self):
        """
        Gets a list of objects currently in the Splitgraph cache (i.e. not only existing externally.)

        :return: Set of object IDs.
        """
        # Minor normalization sadness here: this can return duplicate object IDs since
        # we might repeat them if different versions of the same table point to the same object ID.
        # Also, maybe we should consider looking at the cache status table instead.
        return set(
            self.object_engine.run_sql(
                SQL("""SELECT information_schema.tables.table_name FROM information_schema.tables JOIN {}.tables
                            ON information_schema.tables.table_name = {}.tables.object_id
                            WHERE information_schema.tables.table_schema = %s""")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA),
                            Identifier(SPLITGRAPH_META_SCHEMA)),
                (SPLITGRAPH_META_SCHEMA,), return_shape=ResultShape.MANY_ONE))

    def get_external_object_locations(self, objects):
        """
        Gets external locations for objects.

        :param objects: List of objects stored externally.
        :return: List of (object_id, location, protocol).
        """
        return self.object_engine.run_sql(select("object_locations", "object_id, location, protocol",
                                                 "object_id IN (" + ','.join('%s' for _ in objects) + ")"),
                                          objects)

    def get_object_meta(self, objects):
        """
        Get metadata for multiple Splitgraph objects from the tree

        :param objects: List of objects to get metadata for.
        :return: List of (object_id, format, parent_id, namespace, size).
        """
        return self.object_engine.run_sql(select("objects", "object_id, format, parent_id, namespace, size",
                                                 "object_id IN (" + ','.join('%s' for _ in objects) + ")"), objects)

    def register_table(self, repository, table, image, schema, object_id):
        """
        Registers the object that represents a Splitgraph table inside of an image.

        :param repository: Repository
        :param table: Table name
        :param image: Image hash
        :param schema: Table schema
        :param object_id: Object ID to register the table to.
        """
        self.object_engine.run_sql(
            insert("tables", ("namespace", "repository", "image_hash", "table_name", "table_schema", "object_id")),
            (repository.namespace, repository.repository, image, table, Json(schema), object_id))

    def record_table_as_diff(self, old_table, image_hash):
        """
        Flushes the pending changes from the audit table for a given table and records them,
        registering the new objects.

        :param old_table: Table object pointing to the current HEAD table
        :param image_hash: Image hash to store the table under
        """
        object_id = get_random_object_id()
        engine = self.object_engine
        # Accumulate the diff in-memory. This might become a bottleneck in the future.
        changeset = {}
        for action, row_data, changed_fields in engine.get_pending_changes(old_table.repository.to_schema(),
                                                                           old_table.table_name):
            _conflate_changes(changeset, [(action, row_data, changed_fields)])
        engine.discard_pending_changes(old_table.repository.to_schema(), old_table.table_name)
        changeset = [tuple(list(pk) + [kind_data[0], Json(kind_data[1])]) for pk, kind_data in
                     changeset.items()]

        if changeset:
            engine.store_diff_object(changeset, SPLITGRAPH_META_SCHEMA, object_id,
                                     change_key=engine.get_change_key(old_table.repository.to_schema(),
                                                                      old_table.table_name))
            for parent_id, _ in old_table.objects:
                self.register_object(
                    object_id, object_format='DIFF', namespace=old_table.repository.namespace, parent_object=parent_id)

            self.register_table(old_table.repository, old_table.table_name, image_hash,
                                old_table.table_schema, object_id)
        else:
            # Changes in the audit log cancelled each other out. Delete the diff table and just point
            # the commit to the old table objects.
            for prev_object_id, _ in old_table.objects:
                self.register_table(old_table.repository, old_table.table_name, image_hash,
                                    old_table.table_schema, prev_object_id)

    def record_table_as_snap(self, repository, table_name, image_hash):
        """
        Copies the full table verbatim into a new Splitgraph SNAP object, registering the new object.

        :param repository: Repository
        :param table_name: Table name
        :param image_hash: Hash of the new image
        """
        # Make sure the SNAP for this table doesn't already exist
        table = repository.images.by_hash(image_hash).get_table(table_name)
        if table and table.get_object('SNAP'):
            return

        object_id = get_random_object_id()
        self.object_engine.copy_table(repository.to_schema(), table_name, SPLITGRAPH_META_SCHEMA, object_id,
                                      with_pk_constraints=True)
        self.register_object(object_id, object_format='SNAP', namespace=repository.namespace,
                             parent_object=None)
        table_schema = self.object_engine.get_full_table_schema(repository.to_schema(), table_name)
        self.register_table(repository, table_name, image_hash, table_schema, object_id)

    def extract_recursive_object_meta(self, remote, table_meta):
        """Recursively crawl the a remote object manager in order to fetch all objects
        required to materialize tables specified in `table_meta` that don't yet exist on the local engine."""
        existing_objects = self.get_existing_objects()
        distinct_objects = set(o[3] for o in table_meta if o[3] not in existing_objects)
        known_objects = set()
        object_meta = []

        while True:
            new_parents = [o for o in distinct_objects if o not in known_objects]
            if not new_parents:
                break
            else:
                parents_meta = remote.get_object_meta(new_parents)
                distinct_objects.update(
                    set(o[3] for o in parents_meta if o[3] is not None and o[3] not in existing_objects))
                object_meta.extend(parents_meta)
                known_objects.update(new_parents)
        return distinct_objects, object_meta

    @staticmethod
    def _get_image_object_path(table, object_tree):
        """
        Calculates a list of objects SNAP, DIFF, ... , DIFF that are used to reconstruct a table.

        :param table: Table object
        :param object_tree: Full object tree
        :return: A tuple of (SNAP object, list of DIFF objects in reverse order (latest object first))
        """
        path = []
        object_id = table.get_object('SNAP')
        if object_id is not None:
            return object_id, path

        object_id = table.get_object('DIFF')

        # Here, we have to follow the object tree up until we encounter a parent of type SNAP -- firing a query
        # for every object is a massive bottleneck.
        # This could be done with a recursive PG query in the future, but currently we just load the whole tree
        # and crawl it in memory.
        while object_id is not None:
            path.append(object_id)
            for parent_id in object_tree[object_id][0]:
                # Check the _parent_'s format -- if it's a SNAP, we're done
                if object_tree[parent_id][1] == 'SNAP':
                    return parent_id, path
                object_id = parent_id
                break  # Found 1 diff, will be added to the path at the next iteration.

        # We didn't find an actual snapshot for this table -- something's wrong with the object tree.
        raise SplitGraphException("Couldn't find a SNAP object for %s (malformed object tree)" % table)

    @contextmanager
    def ensure_objects(self, table):
        """
        Resolves the objects needed to materialize a given table and makes sure they are in the local
        splitgraph_meta schema.

        Whilst inside this manager, the objects are guaranteed to exist. On exit from, the objects are marked as
        unneeded and can be garbage collected.

        :param table: Table to materialize
        :return: (SNAP object ID, list of DIFFs to apply to the SNAP (can be empty))
        """

        # Main cache management issue here is concurrency: since we have multiple processes per connection on the
        # server side, if we're accessing this from the FDW, multiple ObjectManagers can fiddle around in the cache
        # status table, triggering weird concurrency cases like:
        #   * We need to download some objects -- another manager wants the same objects. How do we make sure we don't
        #     download them twice? Do we wait on a row-level lock? What happens if another manager crashes?
        #   * We have decided which objects we need -- how do we make sure we don't get evicted by another manager
        #     between us making that decision and increasing the refcount?
        #   * What happens if we crash when we're downloading these objects?

        # So, for now, we lock the whole cache status table. This means that multiple FDWs can't download objects
        # at the same time, but we can try and saturate the download bandwidth. In the future, we can try mode
        # granular row-level locking.
        logging.info("Resolving objects for table %s:%s", table.repository, table.table_name)
        self.object_engine.lock_table(SPLITGRAPH_META_SCHEMA, "object_cache_status")

        object_tree = self.get_full_object_tree()

        # Resolve the table into a list of objects we want to fetch (one SNAP and optionally a list of DIFFs).
        # In the future, we can also take other things into account, such as how expensive it is to load a given object
        # (its size), consider creating a SNAP for tables that are often hit etc.

        # * Questions:
        #   * when do we evict a SNAP? (as opposed to the old SNAP + DIFF chain -- note that chain is obviously bigger
        #     in size than the final SNAP but it lets us access more versions.
        #     * Maybe this solves itself by LRU (if the DIFF chain gets hit more, we'll be more likely to
        #       evict the SNAP.
        #   * Where do we store these SNAPs and how do we incorporate them into the object resolution (as in,
        #     without just registering them in the object tree since then other clients will think they can use that
        #     temporary object)
        snap, diffs = self._get_image_object_path(table, object_tree)
        required_objects = diffs + [snap]
        downloaded_objects = self.get_downloaded_objects()
        to_fetch = set(required_objects).difference(downloaded_objects)

        required_space = sum(object_tree[o][2] for o in to_fetch)
        current_occupied = sum(object_tree[o][2] for o in downloaded_objects)

        logging.info("Need to download %d object(s) (%s), cache occupancy: %s/%s",
                     len(to_fetch), pretty_size(required_space),
                     pretty_size(current_occupied), pretty_size(self.cache_size))

        # If the total cache size isn't large enough, there's nothing we can do without cooperating with the
        # caller and seeing if they can use the objects one-by-one.
        if required_space > self.cache_size:
            raise SplitGraphException("Not enough free space in the cache to download the required objects!")
        if required_space > self.cache_size - current_occupied:
            self.run_eviction(object_tree, to_fetch, required_space + current_occupied - self.cache_size)

        # Perform the actual download. If the table has no upstream but still has external locations, we download
        # just the external objects.
        upstream = table.repository.get_upstream()
        object_locations = self.get_external_object_locations(required_objects)
        self.download_objects(upstream.objects if upstream else None, objects_to_fetch=to_fetch,
                              object_locations=object_locations)
        difference = set(to_fetch).difference(set(self.get_downloaded_objects()))
        if difference:
            logging.exception("Not all objects required to materialize %s:%s:%s have been fetched. Missing objects: %r",
                              table.repository.to_schema(0), table.image.image_hash, table.table_name, difference)
            raise SplitGraphException()

        # We'll probably need to collect some stats here: which objects/tables were hit and when, for example, to guide
        # future SNAP caching.
        # Possibly: a table of (namespace, repository, image_hash, table, timestamp acquired, timestamp released)
        # and occasionally when looking at a new table, we look at the last N minutes of activity and see if we
        # should SNAP it instead.

        # Increase the refcount on all of the objects we're giving back to the caller so that others don't GC them.
        self._update_refcount(required_objects, increment=True)

        try:
            # Release the lock and yield to the caller.
            self.object_engine.commit()
            yield snap, list(reversed(diffs))
        finally:
            # Decrease the refcounts on the objects. Optionally, evict them.
            # If the caller crashes, we should still hit this and decrease the refcounts, but if the whole program
            # is terminated abnormally, we'll leak memory.
            self._update_refcount(required_objects, increment=False)
            self.object_engine.commit()

    def _update_refcount(self, objects, increment=True):
        self.object_engine.run_sql(SQL("UPDATE {}.{} SET refcount = refcount " +
                                       ("+" if increment else "-") + " 1 WHERE object_id IN (" +
                                       ",".join(itertools.repeat("%s", len(objects))) + ")")
                                   .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("object_cache_status")),
                                   objects)

    def run_eviction(self, object_tree, keep_objects, required_space):
        """
        Delete enough objects with zero reference count (only those, since we guarantee that whilst refcount is >0,
        the object stays alive) to free at least `required_space` in the cache.

        :param object_tree: Object tree dictionary
        :param keep_objects: List of objects (besides those with nonzero refcount) to be deleted.
        :param required_space: Space, in bytes, to free. If the routine can't free at least this much space,
            it shall raise an exception.
        :return:
        """
        logging.info("Performing eviction...")
        # Maybe here we should also do the old cleanup (see if the objects aren't required
        #   by any of the current repositories at all).
        # Lots of ways to improve this in the future: approximate the utility of keeping the object
        #   (cost of re-loading * probability of re-loading), LRU, how many tables the object is used in...
        # Currently, do the most basic thing and delete all objects with 0 refcount.
        # Add object size to external_objects too?
        to_delete = self.object_engine.run_sql(select("object_cache_status", "object_id", "refcount=0"),
                                               return_shape=ResultShape.ONE_MANY)
        # Make sure we don't actually delete the objects we'll need.
        to_delete = [o for o in to_delete if o not in keep_objects]
        freed_space = sum(object_tree[o][2] for o in to_delete)
        logging.info("Will delete %d object(s), total size %s", to_delete, freed_space)
        if freed_space < required_space:
            raise SplitGraphException("Not enough space will be reclaimed after eviction!")
        self.delete_objects(to_delete)

    def download_objects(self, source, objects_to_fetch, object_locations):
        """
        Fetches the required objects from the remote and stores them locally.
        Does nothing for objects that already exist.

        :param source: Remote ObjectManager. If None, will only try to download objects from the external location.
        :param objects_to_fetch: List of object IDs to download.
        :param object_locations: List of custom object locations, encoded as tuples (object_id, object_url, protocol).
        """

        existing_objects = self.get_downloaded_objects()
        objects_to_fetch = list(set(o for o in objects_to_fetch if o not in existing_objects))
        if not objects_to_fetch:
            return

        total_size = sum(o[4] for o in self.get_object_meta(objects_to_fetch))
        logging.info("Fetching %d object(s), total size %s", len(objects_to_fetch), pretty_size(total_size))

        # We don't actually seem to pass extra handler parameters when downloading objects since
        # we can have multiple handlers in this batch.
        external_objects = _fetch_external_objects(self.object_engine, object_locations, objects_to_fetch, {})

        remaining_objects_to_fetch = [o for o in objects_to_fetch if o not in external_objects]
        if not remaining_objects_to_fetch or not source:
            return

        self.object_engine.download_objects(remaining_objects_to_fetch, source.object_engine)
        return

    def upload_objects(self, target, objects_to_push, handler='DB', handler_params=None):
        """
        Uploads physical objects to the remote or some other external location.

        :param target: Target ObjectManager
        :param objects_to_push: List of object IDs to upload.
        :param handler: Name of the handler to use to upload objects. Use `DB` to push them to the remote, `FILE`
            to store them in a directory that can be accessed from the client and `HTTP` to upload them to HTTP.
        :param handler_params: For `HTTP`, a dictionary `{"username": username, "password", password}`. For `FILE`,
            a dictionary `{"path": path}` specifying the directory where the objects shall be saved.
        :return: A list of (object_id, url, handler) that specifies all objects were uploaded (skipping objects that
            already exist on the remote).
        """
        if handler_params is None:
            handler_params = {}

        # Get objects that exist on the remote engine
        existing_objects = target.get_existing_objects()

        objects_to_push = list(set(o for o in objects_to_push if o not in existing_objects))
        if not objects_to_push:
            logging.info("Nothing to upload.")
            return []
        total_size = sum(o[4] for o in self.get_object_meta(objects_to_push))
        logging.info("Uploading %d object(s), total size %s", len(objects_to_push), pretty_size(total_size))

        if handler == 'DB':
            self.object_engine.upload_objects(objects_to_push, target.object_engine)
            # We assume that if the object doesn't have an explicit location, it lives on the remote.
            return []

        external_handler = get_external_object_handler(handler, handler_params)
        with switch_engine(self.object_engine):
            uploaded = external_handler.upload_objects(objects_to_push)
        return [(oid, url, handler) for oid, url in zip(objects_to_push, uploaded)]

    def cleanup(self):
        """
        Deletes all objects in the object_tree not required by any current repository, including their dependencies and
        their remote locations. Also deletes all objects not registered in the object_tree.
        """
        # First, get a list of all objects required by a table.
        primary_objects = set(self.object_engine.run_sql(
            SQL("SELECT DISTINCT (object_id) FROM {}.tables").format(Identifier(SPLITGRAPH_META_SCHEMA)),
            return_shape=ResultShape.MANY_ONE))

        # Expand that since each object might have a parent it depends on.
        if primary_objects:
            while True:
                new_parents = set(parent_id for _, _, parent_id, _, _ in self.get_object_meta(list(primary_objects))
                                  if parent_id not in primary_objects and parent_id is not None)
                if not new_parents:
                    break
                else:
                    primary_objects.update(new_parents)

        # Go through the tables that aren't mountpoint-dependent and delete entries there.
        for table_name in ['objects', 'object_locations', 'object_cache_status']:
            query = SQL("DELETE FROM {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(table_name))
            if primary_objects:
                query += SQL(" WHERE object_id NOT IN (" + ','.join('%s' for _ in range(len(primary_objects))) + ")")
            self.object_engine.run_sql(query, list(primary_objects))

        # Go through the physical objects and delete them as well
        # This is slightly dirty, but since the info about the objects was deleted on rm, we just say that
        # anything in splitgraph_meta that's not a system table is fair game.
        tables_in_meta = {c for c in self.object_engine.get_all_tables(SPLITGRAPH_META_SCHEMA) if c not in META_TABLES}

        to_delete = tables_in_meta.difference(primary_objects)
        self.delete_objects(to_delete)
        return to_delete

    def delete_objects(self, objects):
        """
        Deletes objects from the Splitgraph cache

        :param objects: A sequence of objects to be deleted
        """
        for o in objects:
            self.object_engine.delete_table(SPLITGRAPH_META_SCHEMA, o)


def _fetch_external_objects(engine, object_locations, objects_to_fetch, handler_params):
    non_remote_objects = []
    non_remote_by_method = defaultdict(list)
    for object_id, object_url, protocol in object_locations:
        if object_id in objects_to_fetch:
            non_remote_by_method[protocol].append((object_id, object_url))
            non_remote_objects.append(object_id)
    if non_remote_objects:
        logging.info("Fetching external objects...")
        for method, objects in non_remote_by_method.items():
            handler = get_external_object_handler(method, handler_params)
            # In case we're calling this from inside the FDW
            with switch_engine(engine):
                handler.download_objects(objects)
    return non_remote_objects


def _merge_changes(old_change_data, new_change_data):
    old_change_data = {k: v for k, v in zip(old_change_data['c'], old_change_data['v'])}
    old_change_data.update({k: v for k, v in zip(new_change_data['c'], new_change_data['v'])})
    return {'c': list(old_change_data.keys()), 'v': list(old_change_data.values())}


def _conflate_changes(changeset, new_changes):
    """
    Updates a changeset to incorporate the new changes. Assumes that the new changes are non-pk changing
    (e.g. PK-changing updates have been converted into a del + ins).
    """
    for change_pk, change_kind, change_data in new_changes:
        old_change = changeset.get(change_pk)
        if not old_change:
            changeset[change_pk] = (change_kind, change_data)
        else:
            if change_kind == 0:
                if old_change[0] == 1:  # Insert over delete: change to update
                    if change_data == {'c': [], 'v': []}:
                        del changeset[change_pk]
                    else:
                        changeset[change_pk] = (2, change_data)
                else:
                    raise SplitGraphException("Malformed audit log: existing PK %s inserted." % str(change_pk))
            elif change_kind == 1:  # Delete over insert/update: remove the old change
                del changeset[change_pk]
                if old_change[0] == 2:
                    # If it was an update, also remove the old row.
                    changeset[change_pk] = (1, change_data)
                if old_change[0] == 1:
                    # Delete over delete: can't happen.
                    raise SplitGraphException("Malformed audit log: deleted PK %s deleted again" % str(change_pk))
            elif change_kind == 2:  # Update over insert/update: merge the two changes.
                if old_change[0] == 0 or old_change[0] == 2:
                    new_data = _merge_changes(old_change[1], change_data)
                    changeset[change_pk] = (old_change[0], new_data)


def get_random_object_id():
    """Assign each table a random ID that it will be stored as. Note that postgres limits table names to 63 characters,
    so the IDs shall be 248-bit strings, hex-encoded, + a letter prefix since Postgres doesn't seem to support table
    names starting with a digit."""
    # Make sure we're padded to 62 characters (otherwise if the random number generated is less than 2^247 we'll be
    # dropping characters from the hex format)
    return str.format('o{:062x}', getrandbits(248))
