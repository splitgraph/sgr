import bisect
from random import getrandbits

from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier
from splitgraph.engine.postgres.engine import SG_UD_FLAG

from ._common import adapt, SPLITGRAPH_META_SCHEMA, ResultShape, insert, coerce_val_to_json

# PG types we can run max/min on
_PG_INDEXABLE_TYPES = [
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
    "timestamp with time zone"]


def _split_changeset(changeset, min_max, table_pks):
    # maybe order min/max here
    maxs = [m[1] for m in min_max]
    changesets_by_segment = [{} for _ in range(len(min_max))]
    before_changesets = {}
    after_changesets = {}
    for pk, data in changeset.items():
        pk = tuple(adapt(v, p[1]) for v, p in zip(pk, table_pks))
        if min_max[0][0] is None or pk < min_max[0][0]:
            before_changesets[pk] = data
            continue
        if pk > min_max[-1][1]:
            after_changesets[pk] = data
            continue
        # This change matches one of the chunks
        changesets_by_segment[bisect.bisect_left(maxs, pk)][pk] = data
    return changesets_by_segment, before_changesets, after_changesets


class FragmentManager:
    """
    A storage engine for Splitgraph tables. Each table can be stored as one or more immutable fragments that can
    optionally overwrite each other. When a new table is created, it's split up into multiple base fragments. When
    a new version of the table is written, the audit log is inspected and one or more patch fragments are created,
    to be based on the fragments the previous version of the table consisted of. Only the top fragments in this stack
    are stored in the table metadata: to reconstruct the whole table, the links from the top fragments down to the
    base fragments have to be followed.

    In addition, the fragments engine also supports min-max indexing on fragments: this is used to only fetch fragments
    that are required for a given query.
    """

    def __init__(self, object_engine):
        self.object_engine = object_engine

    def _generate_object_index(self, object_id, changeset=None):
        """
        Queries the max/min values of a given fragment for each column, used to speed up querying.

        :param object_id: ID of an object
        :param changeset: Optional, if specified, the old row values are included in the index.
        :return: Dict of {column_name: (min_val, max_val)}
        """
        # Maybe we should pass the column names in instead?
        columns = {c[1]: c[2] for c in self.object_engine.get_full_table_schema(SPLITGRAPH_META_SCHEMA, object_id)
                   if c[1] != SG_UD_FLAG and c[2] in _PG_INDEXABLE_TYPES}

        query = SQL("SELECT ") + SQL(",").join(SQL("MIN({0}), MAX({0})").format(Identifier(c)) for c in columns)
        query += SQL(" FROM {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id))
        result = self.object_engine.run_sql(query, return_shape=ResultShape.ONE_MANY)

        index = {col: (cmin, cmax) for col, cmin, cmax in zip(columns, result[0::2], result[1::2])}

        if changeset:
            # Expand the index ranges to include the old row values in this chunk.
            # Why is this necessary? Say we have a table of (key (PK), value) and a
            # query "value = 42". Say we have 2 objects:
            #
            #   key | value
            #   1   | 42
            #
            #   key | value
            #   1   | 43   (UPDATED)
            #
            # If we don't include the old value that object 2 overwrote in the index, we'll disregard object 2
            # when inspecting the index for that query (since there, "value" only spans [43, 43]) and give the
            # wrong answer (1, 42) even though we should give (1, 43). Similarly with deletes: if the index for
            # an object doesn't say "some of the values spanning this range are deleted in this chunk",
            # we won't fetch the object.
            #
            # See test_lq_qual_filtering for these test cases.

            # For DELETEs, we put NULLs in the non-PK columns; make sure we ignore them here.
            def _min(a, b):
                return b if a is None else (a if b is None else min(a, b))

            def _max(a, b):
                return b if a is None else (a if b is None else max(a, b))

            for _, old_row in changeset.values():
                for col, val in old_row.items():
                    # Ignore columns that we aren't indexing because they have unsupported types.
                    # Also ignore NULL values.
                    if col not in columns or val is None:
                        continue
                    # The audit trigger stores the old row values as JSON so only supports strings and floats/ints.
                    # Hence, we have to coerce them into the values returned by the index.
                    val = adapt(val, columns[col])
                    index[col] = (_min(index[col][0], val),
                                  _max(index[col][1], val))

        index = {k: (coerce_val_to_json(v[0]), coerce_val_to_json(v[1])) for k, v in index.items()}
        return index

    def register_object(self, object_id, object_format, namespace, parent_object=None, changeset=None):
        """
        Registers a Splitgraph object in the object tree and indexes it

        :param object_id: Object ID
        :param object_format: Format (SNAP or DIFF)
        :param namespace: Namespace that owns the object. In registry mode, only namespace owners can alter or delete
            objects.
        :param parent_object: Parent that the object depends on, if it's a DIFF object.
        :param changeset: For DIFF objects, changeset that produced this object. Must be a dictionary of
            {PK: (True for upserted/False for deleted, old row (if updated or deleted))}. The old values
            are used to generate the min/max index for an object to know if it removes/updates some rows
            that might be pertinent to a query.
        """
        if not parent_object and object_format != 'SNAP':
            raise ValueError("Non-SNAP objects can't have no parent!")
        if parent_object and object_format == 'SNAP':
            raise ValueError("SNAP objects can't have a parent!")

        object_size = self.object_engine.get_table_size(SPLITGRAPH_META_SCHEMA, object_id)
        object_index = self._generate_object_index(object_id, changeset)
        self.object_engine.run_sql(
            insert("objects", ("object_id", "format", "parent_id", "namespace", "size", "index")),
            (object_id, object_format, parent_object, namespace, object_size, object_index))

    def register_table(self, repository, table, image, schema, object_ids):
        """
        Registers the object that represents a Splitgraph table inside of an image.

        :param repository: Repository
        :param table: Table name
        :param image: Image hash
        :param schema: Table schema
        :param object_ids: IDs of fragments that the table is composed of
        """
        self.object_engine.run_sql(
            insert("tables", ("namespace", "repository", "image_hash", "table_name", "table_schema", "object_ids")),
            (repository.namespace, repository.repository, image, table, Json(schema), object_ids))

    def record_table_as_patch(self, old_table, image_hash, split_changeset=False):
        """
        Flushes the pending changes from the audit table for a given table and records them,
        registering the new objects.

        :param old_table: Table object pointing to the current HEAD table
        :param image_hash: Image hash to store the table under
        :param split_changeset: See `Repository.commit` for reference
        """

        # TODO does the reasoning in the docstring actually make sense? If the point is to, say, for a query
        # (pk=5000) fetch 0 fragments instead of 1 small one, is it worth the cost of having 2 extra fragments?
        # If a changeset is really large (e.g. update the first 1000 rows, update the last 1000 rows) then sure,
        # this will help (for a query pk=5000 we don't need to fetch a 2000-row fragment) but maybe at that point
        # it's time to rewrite the table altogether?

        engine = self.object_engine

        # Accumulate the diff in-memory. This might become a bottleneck in the future.
        changeset = {}
        _conflate_changes(changeset, engine.get_pending_changes(old_table.repository.to_schema(),
                                                                old_table.table_name))
        engine.discard_pending_changes(old_table.repository.to_schema(), old_table.table_name)
        if changeset:
            # Get all the top fragments that the current table depends on
            top_fragments = old_table.objects

            if split_changeset:
                # Follow the chains down to the base fragments that we'll use to find the chunk boundaries
                base_fragments = [self.get_all_required_objects(o)[-1] for o in top_fragments]

                table_pks = self.object_engine.get_change_key(old_table.repository.to_schema(),
                                                              old_table.table_name)
                min_max = self._extract_min_max_pks(base_fragments, table_pks)

                matched, before, after = _split_changeset(changeset, min_max, table_pks)
            else:
                # Link the change to the final region
                matched = [{} for _ in range(len(top_fragments) - 1)] + [changeset]
                before = {}
                after = {}

            # Register changes that match chunks
            object_ids = []
            for ix, sub_changeset in enumerate(matched):
                # If chunk unchanged, link the table to the same fragment
                if not sub_changeset:
                    object_ids.append(top_fragments[ix])
                    continue
                object_id = get_random_object_id()
                object_ids.append(object_id)
                upserted = [pk for pk, data in sub_changeset.items() if data[0]]
                deleted = [pk for pk, data in sub_changeset.items() if not data[0]]
                engine.store_fragment(upserted, deleted, SPLITGRAPH_META_SCHEMA, object_id,
                                      old_table.repository.to_schema(),
                                      old_table.table_name)
                self.register_object(
                    object_id, object_format='DIFF', namespace=old_table.repository.namespace,
                    parent_object=top_fragments[ix], changeset=sub_changeset)

            # Register the changes that don't match chunks
            # TODO these are very similar, collapse into a loop
            if before:
                object_id = get_random_object_id()
                object_ids = [object_id] + object_ids
                # As this fragment didn't match any other boundaries, it must be an insert
                engine.store_fragment(list(before.keys()), [], SPLITGRAPH_META_SCHEMA, object_id,
                                      old_table.repository.to_schema(),
                                      old_table.table_name)
                # Technically this is a DIFF (since it has the UD column) but we ignore that col anyway?
                self.register_object(
                    object_id, object_format='SNAP', namespace=old_table.repository.namespace,
                    parent_object=None, changeset=before)
            if after:
                object_id = get_random_object_id()
                object_ids.append(object_id)
                engine.store_fragment(list(after.keys()), [], SPLITGRAPH_META_SCHEMA, object_id,
                                      old_table.repository.to_schema(),
                                      old_table.table_name)
                self.register_object(
                    object_id, object_format='SNAP', namespace=old_table.repository.namespace,
                    parent_object=None, changeset=after)

            # Finally, link the table to the new set of objects
            self.register_table(old_table.repository, old_table.table_name, image_hash,
                                old_table.table_schema, object_ids)
        else:
            # Changes in the audit log cancelled each other out. Point the image to the same old objects.
            self.register_table(old_table.repository, old_table.table_name, image_hash,
                                old_table.table_schema, old_table.objects)

    def _extract_min_max_pks(self, fragments, table_pks):
        # Get the min/max PK values for every chunk
        # Why can't we use the index here? If the PK is composite, consider this example:
        #
        # (1, 1)   CHUNK 1
        # (1, 2)   <- pk1: min 1, max 2; pk2: min1, max2 but (pk1, pk2): min (1, 1), max (2, 1)
        # (2, 1)
        # ------   CHUNK 2
        # (2, 2)   <- pk1: min 2, max 2; pk2: min 2, max 2;
        #
        # Say we have a changeset doing UPDATE pk=(2,2). If we use the index by each part of the key separately,
        # it fits both the first and the second chunk. This essentially means that chunks now overlap:
        # we now have to apply chunk 2 (and everything inheriting from it) after chunk 1 (and everything that
        # inherits from it) and make sure to attach the new fragment to chunk 2.
        # This could be solved by including composite PKs in the index as well, not just individual columns.
        # Currently, we assume the objects are local so doing this is mostly OK but that's a strong assumption
        # (there isn't much preventing us from evicting objects once they've been used to materialize a table,
        # so if we do that, we won't want to redownload them again just to find their boundaries).

        min_max = []
        pk_sql = SQL(",").join(Identifier(p[0]) for p in table_pks)
        for o in fragments:
            query = SQL("SELECT ") + pk_sql + SQL(" FROM {}.{} ORDER BY ").format(
                Identifier(SPLITGRAPH_META_SCHEMA), Identifier(o))
            frag_min = self.object_engine.run_sql(query + pk_sql + SQL(" LIMIT 1"),
                                                  return_shape=ResultShape.ONE_MANY)
            frag_max = self.object_engine.run_sql(query +
                                                  SQL(",").join(Identifier(p[0]) + SQL(" DESC")
                                                                for p in table_pks)
                                                  + SQL(" LIMIT 1"), return_shape=ResultShape.ONE_MANY)

            min_max.append((frag_min, frag_max))
        return min_max

    def record_table_as_base(self, repository, table_name, image_hash, chunk_size=10000):
        """
        Copies the full table verbatim into one or more new base fragments and registers them.

        :param repository: Repository
        :param table_name: Table name
        :param image_hash: Hash of the new image
        :param chunk_size: If specified, splits the table into multiple objects with a given number of rows
        """
        object_ids = []
        table_size = self.object_engine.run_sql(SQL("SELECT COUNT (1) FROM {}.{}")
                                                .format(Identifier(repository.to_schema()), Identifier(table_name)),
                                                return_shape=ResultShape.ONE_ONE)
        if chunk_size and table_size:
            for offset in range(0, table_size, chunk_size):
                object_id = get_random_object_id()
                self.object_engine.copy_table(repository.to_schema(), table_name, SPLITGRAPH_META_SCHEMA, object_id,
                                              with_pk_constraints=True, offset=offset, limit=chunk_size)
                self.register_object(object_id, object_format='SNAP', namespace=repository.namespace,
                                     parent_object=None)
                object_ids.append(object_id)
        else:
            object_id = get_random_object_id()
            self.object_engine.copy_table(repository.to_schema(), table_name, SPLITGRAPH_META_SCHEMA, object_id,
                                          with_pk_constraints=True)
            self.register_object(object_id, object_format='SNAP', namespace=repository.namespace,
                                 parent_object=None)
            object_ids = [object_id]
        table_schema = self.object_engine.get_full_table_schema(repository.to_schema(), table_name)
        self.register_table(repository, table_name, image_hash, table_schema, object_ids)

    def get_all_required_objects(self, object_id):
        parents = self.object_engine.run_sql(SQL(
            """WITH RECURSIVE parents AS (
                SELECT object_id, parent_id FROM {0}.objects WHERE object_id = %s
                UNION ALL
                    SELECT o.object_id, o.parent_id
                        FROM parents p JOIN {0}.objects o ON p.parent_id = o.object_id)
            SELECT object_id FROM parents""").format(Identifier(SPLITGRAPH_META_SCHEMA)), (object_id,),
                                             return_shape=ResultShape.MANY_ONE)
        return list(parents)


def _conflate_changes(changeset, new_changes):
    """
    Updates a changeset to incorporate the new changes. Assumes that the new changes are non-pk changing
    (i.e. PK-changing updates have been converted into a del + ins).
    """
    for change_pk, upserted, old_row in new_changes:
        old_change = changeset.get(change_pk)
        if not old_change:
            changeset[change_pk] = (upserted, old_row)
        else:
            if upserted and not old_row:
                # INSERT -- can only happen over a DELETE. Mark the row as upserted; no need to keep track
                # of the old row (since we have it).
                changeset[change_pk] = (upserted, old_row)
            if upserted and old_row:
                # UPDATE -- can only happen over another UPDATE or over an INSERT.
                # If it's over an UPDATE, we keep track of the old row; if it's over an INSERT,
                # we can access the inserted row anyway so no point keeping track of it.
                changeset[change_pk] = (upserted, old_change[1])
            if not upserted:
                # DELETE.
                if old_change[1]:
                    # If happened over an UPDATE, we need to remember the value that was there before the UPDATE.
                    changeset[change_pk] = (upserted, old_change[1])
                else:
                    # If happened over an INSERT, it's a no-op (changes cancel out).
                    del changeset[change_pk]

    return changeset


def get_random_object_id():
    """Assign each table a random ID that it will be stored as. Note that postgres limits table names to 63 characters,
    so the IDs shall be 248-bit strings, hex-encoded, + a letter prefix since Postgres doesn't seem to support table
    names starting with a digit."""
    # Make sure we're padded to 62 characters (otherwise if the random number generated is less than 2^247 we'll be
    # dropping characters from the hex format)
    return str.format('o{:062x}', getrandbits(248))
