"""Functions related to creating, deleting and keeping track of physical Splitgraph objects."""
import itertools
import logging
import math
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime as dt

from psycopg2.sql import SQL, Identifier
from splitgraph.config import SPLITGRAPH_META_SCHEMA, CONFIG
from splitgraph.core.fragment_manager import FragmentManager
from splitgraph.core.metadata_manager import MetadataManager
from splitgraph.engine import ResultShape, switch_engine
from splitgraph.exceptions import SplitGraphException
from splitgraph.hooks.external_objects import get_external_object_handler

from ._common import META_TABLES, select, insert, pretty_size, Tracer


class ObjectManager(FragmentManager, MetadataManager):
    """Brings the multiple manager classes together and manages the object cache (downloading and uploading
    objects as required in order to fulfill certain queries)"""

    def __init__(self, object_engine):
        """
        :param object_engine: An ObjectEngine that will be used as a backing store for the
            objects.
        """
        super().__init__(object_engine)

        # Cache size in bytes
        self.cache_size = float(CONFIG['SG_OBJECT_CACHE_SIZE']) * 1024 * 1024

        # 0 to infinity; higher means objects with smaller sizes are more likely to
        # get evicted than objects that haven't been used for a while.
        # Currently calculated so that an object that hasn't been accessed for 5 minutes has the same
        # removal priority as an object twice its size that's just been accessed.
        self.eviction_decay_constant = float(CONFIG['SG_EVICTION_DECAY'])

        # Objects smaller than this size are assumed to have this size (to simulate the latency of
        # downloading them).
        self.eviction_floor = float(CONFIG['SG_EVICTION_FLOOR']) * 1024 * 1024

    def get_full_object_tree(self):
        """Returns a dictionary (object_id -> parent, object_format, size) with the full object tree
        in the engine"""
        query_result = self.object_engine.run_sql(select("objects", "object_id,parent_id,format,size"))

        result = {}
        for object_id, parent_id, object_format, size in query_result:
            result[object_id] = parent_id, object_format, size
        return result

    def get_downloaded_objects(self, limit_to=None):
        """
        Gets a list of objects currently in the Splitgraph cache (i.e. not only existing externally.)

        :param limit_to: If specified, only the objects in this list will be returned.
        :return: Set of object IDs.
        """
        query = "SELECT pg_tables.tablename FROM pg_tables WHERE pg_tables.schemaname = %s"
        query_args = [SPLITGRAPH_META_SCHEMA]
        if limit_to:
            query += " AND pg_tables.tablename IN (" + ",".join(itertools.repeat('%s', len(limit_to))) + ")"
            query_args += list(limit_to)
        objects = set(self.object_engine.run_sql(
            SQL(query).format(Identifier(SPLITGRAPH_META_SCHEMA)), query_args, return_shape=ResultShape.MANY_ONE))
        return objects.difference(META_TABLES)

    def get_cache_occupancy(self):
        """
        :return: Space occupied by objects cached from external locations, in bytes.
        """
        return int(self.object_engine.run_sql(SQL("""
            SELECT COALESCE(SUM(o.size), 0) FROM {0}.object_cache_status oc JOIN {0}.objects o
            ON o.object_id = oc.object_id WHERE oc.ready = 't'""").format(Identifier(SPLITGRAPH_META_SCHEMA)),
                                              return_shape=ResultShape.ONE_ONE))

    @contextmanager
    def ensure_objects(self, table, quals=None):
        """
        Resolves the objects needed to materialize a given table and makes sure they are in the local
        splitgraph_meta schema.

        Whilst inside this manager, the objects are guaranteed to exist. On exit from it, the objects are marked as
        unneeded and can be garbage collected.

        :param table: Table to materialize
        :param quals: Optional list of qualifiers to be passed to the fragment engine. Fragments that definitely do
            not match these qualifiers will be dropped. See the docstring for `filter_fragments` for the format.
        :return: List of table fragments
        """

        # Main cache management issue here is concurrency: since we have multiple processes per connection on the
        # server side, if we're accessing this from the FDW, multiple ObjectManagers can fiddle around in the cache
        # status table, triggering weird concurrency cases like:
        #   * We need to download some objects -- another manager wants the same objects. How do we make sure we don't
        #     download them twice? Do we wait on a row-level lock? What happens if another manager crashes?
        #   * We have decided which objects we need -- how do we make sure we don't get evicted by another manager
        #     between us making that decision and increasing the refcount?
        #   * What happens if we crash when we're downloading these objects?

        logging.info("Resolving objects for table %s:%s:%s", table.repository, table.image.image_hash, table.table_name)

        self.object_engine.run_sql("SET LOCAL synchronous_commit TO OFF")
        tracer = Tracer()

        # Resolve the table into a list of objects we want to fetch.
        # In the future, we can also take other things into account, such as how expensive it is to load a given object
        # (its size), location...
        required_objects = list(reversed(self.get_all_required_objects(table.objects)))
        tracer.log('resolve_objects')

        # Filter to see if we can discard any objects with the quals
        required_objects = self._filter_objects(required_objects, table, quals)
        tracer.log('filter_objects')

        # Increase the refcount on all of the objects we're giving back to the caller so that others don't GC them.
        logging.info("Claiming %d object(s)", len(required_objects))

        # here: only claim external objects (if an object exists locally and doesn't have an entry in the cache,
        # don't add one)

        self._claim_objects(required_objects)
        tracer.log('claim_objects')
        # This also means that anybody else who tries to claim this set of objects will lock up until we're done with
        # them (though note that if anything commits in the download step, these locks will get released and we'll
        # be liable to be stepped on)

        try:
            to_fetch = self._prepare_fetch_list(required_objects)
        except SplitGraphException:
            self.object_engine.rollback()
            raise
        tracer.log('prepare_fetch_list')

        # Perform the actual download. If the table has no upstream but still has external locations, we download
        # just the external objects.
        if to_fetch:
            upstream = table.repository.upstream
            object_locations = self.get_external_object_locations(required_objects)
            self.download_objects(upstream.objects if upstream else None,
                                  objects_to_fetch=to_fetch, object_locations=object_locations)
            # Can't actually use the list of downloaded objects returned by the routine: if another instance of the
            # object manager downloaded those objects between us compiling a list and performing the download,
            # the downloaded objects won't be in the returned list.
            difference = set(to_fetch).difference(self.get_downloaded_objects(limit_to=to_fetch))
            if difference:
                error = "Not all objects required to materialize %s:%s:%s have been fetched. Missing objects: %r" % \
                        (table.repository.to_schema(), table.image.image_hash, table.table_name, difference)
                logging.error(error)
                self.object_engine.rollback()
                raise SplitGraphException(error)
            self._set_ready_flags(to_fetch, is_ready=True)
        tracer.log('fetch_objects')

        logging.info("Yielding to the caller")
        try:
            # Release the lock and yield to the caller.
            self.object_engine.commit()
            yield required_objects
        finally:
            # Decrease the refcounts on the objects. Optionally, evict them.
            # If the caller crashes, we should still hit this and decrease the refcounts, but if the whole program
            # is terminated abnormally, we'll leak memory.
            tracer.log('caller')
            self.object_engine.run_sql("SET LOCAL synchronous_commit TO off")
            self._release_objects(required_objects)
            tracer.log('release_objects')
            logging.info("Releasing %d object(s)", len(required_objects))
            logging.info("Timing stats for %s/%s/%s/%s: \n%s", table.repository.namespace, table.repository.repository,
                         table.image.image_hash, table.table_name, tracer)
            self.object_engine.commit()

    def _filter_objects(self, objects, table, quals):
        if quals:
            column_types = {c[1]: c[2] for c in table.table_schema}
            filtered_objects = self.filter_fragments(objects, quals, column_types)
            logging.info("Qual filter: discarded %d/%d object(s)",
                         len(objects) - len(filtered_objects), len(objects))
            # Make sure to keep the order
            objects = [r for r in objects if r in filtered_objects]
        return objects

    def _prepare_fetch_list(self, required_objects):
        """
        Calculates the missing objects and ensures there's enough space in the cache
        to download them.

        :param required_objects: Iterable of object IDs that are required to be on the engine.
        :return: Set of objects to fetch
        """
        objects_in_cache = self.get_downloaded_objects(limit_to=required_objects)
        to_fetch = set(required_objects).difference(objects_in_cache)
        if to_fetch:
            required_space = sum(o[4] for o in self.get_object_meta(list(to_fetch)))
            current_occupied = self.get_cache_occupancy()
            logging.info("Need to download %d object(s) (%s), cache occupancy: %s/%s",
                         len(to_fetch), pretty_size(required_space),
                         pretty_size(current_occupied), pretty_size(self.cache_size))
            # If the total cache size isn't large enough, there's nothing we can do without cooperating with the
            # caller and seeing if they can use the objects one-by-one.
            if required_space > self.cache_size:
                raise SplitGraphException("Not enough space in the cache to download the required objects!")
            if required_space > self.cache_size - current_occupied:
                to_free = required_space + current_occupied - self.cache_size
                logging.info("Need to free %s", pretty_size(to_free))
                self.run_eviction(self.get_full_object_tree(), required_objects, to_free)
        return to_fetch

    def _claim_objects(self, objects):
        """Increases refcounts and bumps the last used timestamp to now for cached objects.
        For objects that aren't in the cache, checks that they don't already exist locally and then
        adds them to the cache status table, marking them with ready=False
        (which must be set to True by the end of the operation)."""
        if not objects:
            return
        now = dt.utcnow()
        # Objects that were created locally aren't supposed to be claimed here or have an entry in the cache.
        # So, we first try to update cache entries to bump their refcount, see which ones we updated,
        # subtract objects that we have locally and insert the remaining entries as new cache entries.

        claimed = self.object_engine.run_sql(SQL("UPDATE {}.object_cache_status SET refcount = refcount + 1, "
                                                 "last_used = %s WHERE object_id IN (")
                                             .format(Identifier(SPLITGRAPH_META_SCHEMA))
                                             + SQL(",".join(itertools.repeat("%s", len(objects)))) +
                                             SQL(") RETURNING object_id"), [now] + objects,
                                             return_shape=ResultShape.MANY_ONE)
        claimed = claimed or []
        remaining = set(objects).difference(set(claimed))
        remaining = remaining.difference(set(self.get_downloaded_objects(limit_to=list(remaining))))
        self.object_engine.run_sql_batch(insert("object_cache_status", ("object_id", "ready", "refcount", "last_used")),
                                         [(object_id, False, 1, now) for object_id in remaining])

    def _set_ready_flags(self, objects, is_ready=True):
        if objects:
            self.object_engine.run_sql(SQL("UPDATE {0}.object_cache_status SET ready = %s WHERE object_id IN (" +
                                           ",".join(itertools.repeat("%s", len(objects))) + ")")
                                       .format(Identifier(SPLITGRAPH_META_SCHEMA)), [is_ready] + list(objects))

    def _release_objects(self, objects):
        """Decreases objects' refcounts."""
        if objects:
            self.object_engine.run_sql(SQL("UPDATE {}.{} SET refcount = refcount - 1 WHERE object_id IN (" +
                                           ",".join(itertools.repeat("%s", len(objects))) + ")")
                                       .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("object_cache_status")),
                                       objects)

    def run_eviction(self, object_tree, keep_objects, required_space=None):
        """
        Delete enough objects with zero reference count (only those, since we guarantee that whilst refcount is >0,
        the object stays alive) to free at least `required_space` in the cache.

        :param object_tree: Object tree dictionary
        :param keep_objects: List of objects (besides those with nonzero refcount) that can't be deleted.
        :param required_space: Space, in bytes, to free. If the routine can't free at least this much space,
            it shall raise an exception. If None, removes all eligible objects.
        """

        now = dt.utcnow()

        def _eviction_score(object_size, last_used):
            # We want to evict objects in order to minimize
            # P(object is requested again) * (cost of redownloading the object).
            # To approximate the probability, we use an exponential decay function (1 if last_used = now, dropping down
            # to 0 as time since the object's last usage time passes).
            # To approximate the cost, we use the object's size, floored to a constant (so if the object has
            # size <= floor, we'd use the floor value -- this is to simulate the latency of re-fetching the object,
            # as opposed to the bandwidth)
            time_since_used = (now - last_used).total_seconds()
            time_factor = math.exp(-self.eviction_decay_constant * time_since_used)
            size_factor = object_size if object_size > self.eviction_floor else self.eviction_floor
            return time_factor * size_factor

        logging.info("Performing eviction...")
        # Maybe here we should also do the old cleanup (see if the objects aren't required
        #   by any of the current repositories at all).

        # Find deletion candidates: objects that we have locally, with refcount 0, that aren't in the whitelist.

        # We need to lock the table to make sure that our calculations of what we're about to delete are consistent.
        # However, we can deadlock: we're already holding a lock on some objects and want to acquire an exclusive
        # lock and so will wait for other workers to release their objects -- however, they might be waiting on us
        # to release our objects.

        # Hence, we commit the object refcount increase (so that others can't GC them), releasing the lock,
        # and try to acquire the stronger lock.
        self.object_engine.commit()
        self.object_engine.lock_table(SPLITGRAPH_META_SCHEMA, "object_cache_status")

        candidates = [o for o in self.object_engine.run_sql(
            select("object_cache_status", "object_id,last_used", "refcount=0"),
            return_shape=ResultShape.MANY_MANY) if o[0] not in keep_objects]

        if required_space is None:
            # Just delete everything with refcount 0.
            to_delete = [o[0] for o in candidates]
            freed_space = sum(object_tree[o][2] for o in to_delete)
        else:
            if required_space > sum(object_tree[o[0]][2] for o in candidates):
                raise SplitGraphException("Not enough space will be reclaimed after eviction!")

            # Sort them by deletion priority (lowest is smallest expected retrieval cost -- more likely to delete)
            to_delete = []
            candidates = sorted(candidates,
                                key=lambda o: _eviction_score(object_tree[o[0]][2], o[1]))
            freed_space = 0
            # Keep adding deletion candidates until we've freed enough space.
            for object_id, _ in candidates:
                to_delete.append(object_id)
                freed_space += object_tree[object_id][2]
                if freed_space >= required_space:
                    break

        logging.info("Will delete %d object(s), total size %s", len(to_delete), pretty_size(freed_space))
        self.delete_objects(to_delete)
        if to_delete:
            self.object_engine.run_sql(SQL("DELETE FROM {}.{} WHERE object_id IN (" +
                                           ",".join(itertools.repeat("%s", len(to_delete))) + ")")
                                       .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("object_cache_status")),
                                       to_delete)

        # Release the exclusive lock and relock the objects we want instead once again (???)
        self.object_engine.commit()
        self.object_engine.run_sql("SET LOCAL synchronous_commit TO off;")
        self._release_objects(keep_objects)
        self._claim_objects(keep_objects)

    def download_objects(self, source, objects_to_fetch, object_locations):
        """
        Fetches the required objects from the remote and stores them locally.
        Does nothing for objects that already exist.

        :param source: Remote ObjectManager. If None, will only try to download objects from the external location.
        :param objects_to_fetch: List of object IDs to download.
        :param object_locations: List of custom object locations, encoded as tuples (object_id, object_url, protocol).
        """

        existing_objects = self.get_downloaded_objects(limit_to=objects_to_fetch)
        objects_to_fetch = list(set(o for o in objects_to_fetch if o not in existing_objects))
        if not objects_to_fetch:
            return []

        total_size = sum(o[4] for o in self.get_object_meta(objects_to_fetch))
        logging.info("Fetching %d object(s), total size %s", len(objects_to_fetch), pretty_size(total_size))

        # We don't actually seem to pass extra handler parameters when downloading objects since
        # we can have multiple handlers in this batch.
        external_objects = _fetch_external_objects(self.object_engine, object_locations, objects_to_fetch, {})

        remaining_objects_to_fetch = [o for o in objects_to_fetch if o not in external_objects]
        if not remaining_objects_to_fetch or not source:
            return external_objects

        remote_objects = self.object_engine.download_objects(remaining_objects_to_fetch, source.object_engine)
        return external_objects + remote_objects

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
        primary_objects = {o for os in self.object_engine.run_sql(
            SQL("SELECT object_ids FROM {}.tables").format(Identifier(SPLITGRAPH_META_SCHEMA)),
            return_shape=ResultShape.MANY_ONE) for o in os}

        # Expand that since each object might have a parent it depends on.
        if primary_objects:
            while True:
                new_parents = set(parent_id for _, _, parent_id, _, _, _ in self.get_object_meta(list(primary_objects))
                                  if parent_id not in primary_objects and parent_id is not None)
                if not new_parents:
                    break
                else:
                    primary_objects.update(new_parents)

        # Go through the tables that aren't repository-dependent and delete entries there.
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
        objects = list(objects)
        for i in range(0, len(objects), 100):
            query = SQL(";").join(SQL("DROP TABLE IF EXISTS {}.{}")
                                  .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(o))
                                  for o in objects[i:i + 100])
            self.object_engine.run_sql(query)
            self.object_engine.commit()


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
