"""
Routines related to storing tables as fragments.
"""

import bisect
import itertools
import logging
import operator
import struct
from functools import reduce
from hashlib import sha256
from random import getrandbits

from psycopg2.errors import DuplicateTable, UniqueViolation
from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_API_SCHEMA
from splitgraph.core.metadata_manager import MetadataManager, Object
from splitgraph.engine.postgres.engine import SG_UD_FLAG
from ._common import adapt, SPLITGRAPH_META_SCHEMA, ResultShape, coerce_val_to_json, select

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
    "timestamp with time zone",
]


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


# Custom min/max functions that ignore Nones
def _min(left, right):
    return right if left is None else (left if right is None else min(left, right))


def _max(left, right):
    return right if left is None else (left if right is None else max(left, right))


def get_chunk_groups(chunks):
    """
    Takes a list of chunks and their boundaries and combines them
    into independent groups such that chunks from no two groups
    overlap with each other (intervals are assumed to be closed,
    e.g. chunk (1,2) overlaps with chunk (2,3)).

    The original order of chunks is preserved within each group.

    For example, 4 chunks A, B, C, D that don't overlap each other
    will be grouped into 4 groups `[A], [B], [C], [D]`.

    If A overlaps B, the result will be [A, B], [C], [D].

    If in addition B overlaps C (but not A), the result will be `[A, B, C], [D]`.

    If in addition D overlaps any of A, B or C, the result will be `[A, B, C, D]`
    (despite that D is located before A: it will be last since it was last in the
    original list).

    :param chunks: List of (chunk_id, start, end)
    :return: List of lists of (chunk_id, start, end)
    """
    # Note the original order the chunks came in: it should be preserved
    # within overlap groups.
    chunks = [(i,) + chunk for i, chunk in enumerate(chunks)]

    groups = []
    current_group = []
    current_group_start = None
    current_group_end = None
    for original_id, chunk_id, start, end in sorted(chunks, key=lambda c: c[2]):
        if not current_group:
            current_group = [(original_id, chunk_id, start, end)]
            current_group_start = start
            current_group_end = end
            continue

        # See if the chunk overlaps with the current chunk group
        if start <= current_group_end and end >= current_group_start:
            current_group.append((original_id, chunk_id, start, end))
            current_group_start = min(current_group_start, start)
            current_group_end = max(current_group_end, end)
            continue

        # If the chunk doesn't overlap, we start a new group
        groups.append(current_group)
        current_group = [(original_id, chunk_id, start, end)]
        current_group_start = start
        current_group_end = end

    groups.append(current_group)

    # Resort the chunks within groups again
    return [[c[1:] for c in sorted(chunks)] for chunks in groups]


class Digest:
    """
    Homomorphic hashing similar to LtHash (but limited to being backed by 256-bit hashes). The main property is that
    for any rows A, B, LtHash(A) + LtHash(B) = LtHash(A+B). This is done by construction: we simply hash individual
    rows and then do bit-wise addition / subtraction of individual hashes to come up with the full table hash.

    Hence, the content hash of any Splitgraph table fragment is the sum of hashes of its added rows minus the sum
    of hashes of its deleted rows (including the old values of the rows that have been updated). This has a very
    useful implication: the hash of a full Splitgraph table is equal to the sum of hashes of its individual fragments.

    This property can be used to simplify deduplication.
    """

    def __init__(self, shorts):
        """
        Create a Digest instance.

        :param shorts: Tuple of 16 2-byte integers.
        """
        # Shorts: tuple of 16 2-byte integers
        assert isinstance(shorts, tuple)
        assert len(shorts) == 16
        self.shorts = shorts

    @classmethod
    def empty(cls):
        """Return an empty Digest instance such that for any Digest D, D + empty == D - empty == D"""
        return cls((0,) * 16)

    @classmethod
    def from_memoryview(cls, memory):
        """Create a Digest from a 256-bit memoryview/bytearray."""
        # Unpack the buffer as 16 signed big-endian shortints.
        return cls(struct.unpack(">16H", memory))

    @classmethod
    def from_hex(cls, hex_string):
        """Create a Digest from a 64-characters (256-bit) hexadecimal string"""
        assert len(hex_string) == 64
        return cls(tuple(int(hex_string[i : i + 4], base=16) for i in range(0, 64, 4)))

    # In these routines, we treat each hash as a vector of 16 2-byte integers and do component-wise addition.
    # To simulate the wraparound behaviour of C shorts, throw away all remaining bits after the action.
    def __add__(self, other):
        return Digest(tuple((l + r) & 0xFFFF for l, r in zip(self.shorts, other.shorts)))

    def __sub__(self, other):
        return Digest(tuple((l - r) & 0xFFFF for l, r in zip(self.shorts, other.shorts)))

    def __neg__(self):
        return Digest(tuple(-v & 0xFFFF for v in self.shorts))

    def hex(self):
        """Convert the hash into a hexadecimal value."""
        return struct.pack(">16H", *self.shorts).hex()


class FragmentManager(MetadataManager):
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

    def __init__(self, object_engine, metadata_engine=None):
        super().__init__(object_engine, metadata_engine)
        self.object_engine = object_engine
        self.metadata_engine = metadata_engine or object_engine

    def _generate_object_index(self, object_id, changeset=None):
        """
        Queries the max/min values of a given fragment for each column, used to speed up querying.

        :param object_id: ID of an object
        :param changeset: Optional, if specified, the old row values are included in the index.
        :return: Dict of {column_name: (min_val, max_val)}
        """
        # Maybe we should pass the column names in instead?
        columns = {
            c[1]: c[2]
            for c in self.object_engine.get_full_table_schema(SPLITGRAPH_META_SCHEMA, object_id)
            if c[1] != SG_UD_FLAG and c[2] in _PG_INDEXABLE_TYPES
        }

        query = SQL("SELECT ") + SQL(",").join(
            SQL("MIN({0}), MAX({0})").format(Identifier(c)) for c in columns
        )
        query += SQL(" FROM {}.{}").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
        )
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
            for _, old_row in changeset.values():
                for col, val in old_row.items():
                    # Ignore columns that we aren't indexing because they have unsupported types.
                    # Also ignore NULL values.
                    if col not in columns or val is None:
                        continue
                    # The audit trigger stores the old row values as JSON so only supports strings and floats/ints.
                    # Hence, we have to coerce them into the values returned by the index.
                    val = adapt(val, columns[col])
                    index[col] = (_min(index[col][0], val), _max(index[col][1], val))

        index = {k: (coerce_val_to_json(v[0]), coerce_val_to_json(v[1])) for k, v in index.items()}
        return index

    def _register_object(
        self,
        object_id,
        namespace,
        insertion_hash,
        deletion_hash,
        parent_object=None,
        changeset=None,
    ):
        """
        Registers a Splitgraph object in the object tree and indexes it

        :param object_id: Object ID
        :param namespace: Namespace that owns the object. In registry mode, only namespace owners can alter or delete
            objects.
        :param insertion_hash: Homomorphic hash of all rows inserted by this fragment
        :param deletion_hash: Homomorphic hash of the old values of all rows deleted by this fragment
        :param parent_object: Parent that the object depends on (if it's a patch).
        :param changeset: For patches, changeset that produced this object. Must be a dictionary of
            {PK: (True for upserted/False for deleted, old row (if updated or deleted))}. The old values
            are used to generate the min/max index for an object to know if it removes/updates some rows
            that might be pertinent to a query.
        """
        object_size = self.object_engine.get_object_size(object_id)
        object_index = self._generate_object_index(object_id, changeset)
        self.register_objects(
            [
                Object(
                    object_id=object_id,
                    format="FRAG",
                    parent_id=parent_object,
                    namespace=namespace,
                    size=object_size,
                    insertion_hash=insertion_hash,
                    deletion_hash=deletion_hash,
                    index=object_index,
                )
            ]
        )

    @staticmethod
    def _extract_deleted_rows(changeset, table_schema):
        has_pk = any(c[3] for c in table_schema)
        rows = []
        for pk, data in changeset.items():
            if not data[1]:
                # No old row: new value has been inserted.
                continue
            if not has_pk:
                row = pk
            else:
                # Turn the changeset into an actual row in the correct order
                pk_index = 0
                row = []
                for _, column_name, _, is_pk in sorted(table_schema):
                    if not is_pk:
                        row.append(data[1][column_name])
                    else:
                        row.append(pk[pk_index])
                        pk_index += 1
            rows.append(row)
        return rows

    def _hash_old_changeset_values(self, changeset, table_schema):
        """
        Since we're not storing the values of the deleted rows (and we don't have access to them in the staging table
        because they've been deleted), we have to hash them in Python. This involves mimicking the return value of
        `SELECT t::text FROM table t`.

        :param changeset: Map PK -> (upserted/deleted, Map col -> old val)
        :param table_schema: List (ordinal, column, type, is_pk)
        :return: `Digest` object.
        """
        rows = self._extract_deleted_rows(changeset, table_schema)
        if not rows:
            return Digest.empty()

        # Horror alert: we hash newly created tables by essentially calling digest(row::text) in Postgres and
        # we don't really know how it turns some types to strings. So instead we give Postgres all of its deleted
        # rows back and ask it to hash them for us in the same way.
        inner_tuple = "(" + ",".join("%s::" + c[2] for c in table_schema) + ")"
        query = (
            "SELECT digest(o::text, 'sha256') FROM (VALUES "
            + ",".join(itertools.repeat(inner_tuple, len(rows)))
            + ") o"
        )

        # By default (e.g. for changesets where nothing was deleted) we use a 0 hash (since adding it to any other
        # hash has no effect).
        digests = self.object_engine.run_sql(
            query, [o for row in rows for o in row], return_shape=ResultShape.MANY_ONE
        )
        return reduce(operator.add, map(Digest.from_memoryview, digests), Digest.empty())

    def _store_changesets(self, table, changesets, parents, schema):
        """
        Store and register multiple changesets as fragments, optionally linking them to the parent fragments.

        :param table: Table object the changesets belong to
        :param changesets: List of changeset dictionaries. For empty changesets, will return the parent fragment
            in the list of resulting objects instead.
        :param parents: List of parents to link each new fragment to.
        :param schema: Schema the table is checked out into.
        :return: List of object IDs (created or existing for where the changeset was empty).
        """
        object_ids = []
        for sub_changeset, parent in zip(changesets, parents):
            if not sub_changeset:
                if parent:
                    object_ids.append(parent)
                continue

            # Store the fragment in a temporary location and then find out its hash and rename to the actual target.
            # Optimisation: in the future, we can hash the upserted rows that we need preemptively and possibly
            # avoid storing the object altogether if it's a duplicate.
            tmp_object_id = self._store_changeset(sub_changeset, table.table_name, schema)

            deletion_hash, insertion_hash, object_id = self._get_patch_fragment_hashes(
                sub_changeset, table, tmp_object_id, parent
            )

            object_ids.append(object_id)

            # Wrap this rename in a SAVEPOINT so that if the table already exists,
            # the error doesn't roll back the whole transaction (us creating and registering all other objects).
            with self.object_engine.savepoint("object_rename"):
                try:
                    self.object_engine.store_object(
                        object_id=object_id,
                        source_schema=SPLITGRAPH_META_SCHEMA,
                        source_table=tmp_object_id,
                    )
                except DuplicateTable:
                    # If an object with this ID already exists, delete the temporary table,
                    # don't register it and move on.
                    logging.info(
                        "Reusing object %s for table %s/%s",
                        object_id,
                        table.repository,
                        table.table_name,
                    )
                    self.object_engine.delete_table(SPLITGRAPH_META_SCHEMA, tmp_object_id)

                    # There are some cases where an object can already exist in the object engine (in the cache)
                    # but has been deleted from the metadata engine, so when it's recreated, we'll skip
                    # actually registering it. Hence, we still want to proceed trying to register
                    # it no matter what.

            # Same here: if we are being called as part of a commit and an object
            # already exists, we'll roll back everything that the caller has done
            # (e.g. registering the new image) if we don't have a savepoint.
            with self.metadata_engine.savepoint("object_register"):
                try:
                    self._register_object(
                        object_id,
                        namespace=table.repository.namespace,
                        insertion_hash=insertion_hash.hex(),
                        deletion_hash=deletion_hash.hex(),
                        parent_object=parent,
                        changeset=sub_changeset,
                    )
                except UniqueViolation:
                    logging.info(
                        "Object %s for table %s/%s already exists, continuing...",
                        object_id,
                        table.repository,
                        table.table_name,
                    )

        return object_ids

    def _get_patch_fragment_hashes(self, sub_changeset, table, tmp_object_id, parent_id):
        # Digest the rows.
        deletion_hash = self._hash_old_changeset_values(sub_changeset, table.table_schema)
        insertion_hash = self.calculate_fragment_insertion_hash(
            SPLITGRAPH_META_SCHEMA, tmp_object_id
        )
        content_hash = (insertion_hash - deletion_hash).hex()
        schema_hash = sha256(str(table.table_schema).encode("ascii")).hexdigest()
        object_id = (
            "o"
            + sha256((content_hash + schema_hash + (parent_id or "")).encode("ascii")).hexdigest()[
                :-2
            ]
        )
        return deletion_hash, insertion_hash, object_id

    def _store_changeset(self, sub_changeset, table, schema):
        tmp_object_id = get_random_object_id()
        upserted = [pk for pk, data in sub_changeset.items() if data[0]]
        deleted = [pk for pk, data in sub_changeset.items() if not data[0]]
        self.object_engine.store_fragment(
            upserted, deleted, SPLITGRAPH_META_SCHEMA, tmp_object_id, schema, table
        )
        return tmp_object_id

    def calculate_fragment_insertion_hash(self, schema, table):
        """
        Calculate the homomorphic hash of just the rows that a given fragment inserts
        :param schema: Schema the fragment is stored in.
        :param table: Name of the table the fragment is stored in.
        :return: A `Digest` object
        """
        table_schema = self.object_engine.get_full_table_schema(schema, table)
        columns_sql = SQL(",").join(
            SQL("o.") + Identifier(c[1]) for c in table_schema if c[1] != SG_UD_FLAG
        )
        digest_query = (
            SQL("SELECT digest((")
            + columns_sql
            + SQL(")::text, 'sha256'::text) FROM {}.{} o").format(
                Identifier(schema), Identifier(table)
            )
            + SQL(" WHERE o.{} = true").format(Identifier(SG_UD_FLAG))
        )
        row_digests = self.object_engine.run_sql(digest_query, return_shape=ResultShape.MANY_ONE)
        insertion_hash = reduce(
            operator.add, (Digest.from_memoryview(r) for r in row_digests), Digest.empty()
        )
        return insertion_hash

    def record_table_as_patch(self, old_table, schema, image_hash, split_changeset=False):
        """
        Flushes the pending changes from the audit table for a given table and records them,
        registering the new objects.

        :param old_table: Table object pointing to the current HEAD table
        :param schema: Schema the table is checked out into.
        :param image_hash: Image hash to store the table under
        :param split_changeset: See `Repository.commit` for reference
        """

        # TODO does the reasoning in the docstring actually make sense? If the point is to, say, for a query
        # (pk=5000) fetch 0 fragments instead of 1 small one, is it worth the cost of having 2 extra fragments?
        # If a changeset is really large (e.g. update the first 1000 rows, update the last 1000 rows) then sure,
        # this will help (for a query pk=5000 we don't need to fetch a 2000-row fragment) but maybe at that point
        # it's time to rewrite the table altogether?

        # Accumulate the diff in-memory. This might become a bottleneck in the future.
        changeset = {}
        _conflate_changes(
            changeset, self.object_engine.get_pending_changes(schema, old_table.table_name)
        )
        self.object_engine.discard_pending_changes(schema, old_table.table_name)
        if changeset:
            # Get all the top fragments that the current table depends on
            top_fragments = old_table.objects

            if split_changeset:
                # Follow the chains down to the base fragments that we'll use to find the chunk boundaries
                base_fragments = [self.get_all_required_objects([o])[0] for o in top_fragments]

                table_pks = self.object_engine.get_change_key(schema, old_table.table_name)
                pk_cols = [col for col, _ in table_pks]
                min_max = self.extract_min_max_pks(base_fragments, pk_cols)

                matched, before, after = _split_changeset(changeset, min_max, table_pks)
            else:
                # Link the change to the final region
                matched = [{} for _ in range(len(top_fragments) - 1)] + [changeset]
                before = {}
                after = {}

            # Store the changesets. For the parentless before/after changesets, only create them if they actually
            # contain something.
            # NB test_commit_diff::test_commit_mode_change: in case the before/after changesets actually
            # overwrite something that we didn't detect, push them to be later in the application order.
            object_ids = self._store_changesets(
                old_table, matched + [before, after], top_fragments + [None, None], schema
            )
            # Finally, link the table to the new set of objects.
            self.register_tables(
                old_table.repository,
                [(image_hash, old_table.table_name, old_table.table_schema, object_ids)],
            )
        else:
            # Changes in the audit log cancelled each other out. Point the image to the same old objects.
            self.register_tables(
                old_table.repository,
                [(image_hash, old_table.table_name, old_table.table_schema, old_table.objects)],
            )

    def extract_min_max_pks(self, fragments, table_pks):
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
        pk_sql = SQL(",").join(Identifier(p) for p in table_pks)
        for fragment in fragments:
            query = (
                SQL("SELECT ")
                + pk_sql
                + SQL(" FROM {}.{} ORDER BY ").format(
                    Identifier(SPLITGRAPH_META_SCHEMA), Identifier(fragment)
                )
            )
            frag_min = self.object_engine.run_sql(
                query + pk_sql + SQL(" LIMIT 1"), return_shape=ResultShape.ONE_MANY
            )
            frag_max = self.object_engine.run_sql(
                query
                + SQL(",").join(Identifier(p) + SQL(" DESC") for p in table_pks)
                + SQL(" LIMIT 1"),
                return_shape=ResultShape.ONE_MANY,
            )

            min_max.append((frag_min, frag_max))
        return min_max

    def calculate_content_hash(self, schema, table):
        """
        Calculates the homomorphic hash of table contents.

        :param schema: Schema the table belongs to
        :param table: Name of the table
        :return: A 64-character (256-bit) hexadecimal string with the content hash of the table.
        """
        digest_query = SQL("SELECT digest(o::text, 'sha256'::text) FROM {}.{} o").format(
            Identifier(schema), Identifier(table)
        )
        pks = self.object_engine.get_primary_keys(schema, table)
        if pks:
            digest_query += SQL(" ORDER BY ") + SQL(",").join(Identifier(p[0]) for p in pks)
        row_digests = self.object_engine.run_sql(digest_query, return_shape=ResultShape.MANY_ONE)

        return reduce(operator.add, (Digest.from_memoryview(r) for r in row_digests)).hex()

    def create_base_fragment(
        self, source_schema, source_table, namespace, limit=None, after_pk=None
    ):
        # Store the fragment in a temporary location first and hash that (much faster since PG doesn't need
        # to go through the source table multiple times for every offset)
        tmp_object_id = get_random_object_id()
        logging.info(
            "Using temporary table %s for %s/%s limit %r after_pk %r",
            tmp_object_id,
            source_schema,
            source_table,
            limit,
            after_pk,
        )

        self.object_engine.copy_table(
            source_schema,
            source_table,
            SPLITGRAPH_META_SCHEMA,
            tmp_object_id,
            with_pk_constraints=True,
            limit=limit,
            after_pk=after_pk,
        )
        content_hash = self.calculate_content_hash(SPLITGRAPH_META_SCHEMA, tmp_object_id)

        # Fragments can't be reused in tables with different schemas even if the contents match (e.g. '1' vs 1).
        # Hence, include the table schema in the object ID as well.
        schema_hash = sha256(
            str(self.object_engine.get_full_table_schema(source_schema, source_table)).encode(
                "ascii"
            )
        ).hexdigest()

        # Object IDs are also used to key tables in Postgres so they can't be more than 63 characters.
        # In addition, table names can't start with a number so we have to drop 2 characters from the 64-character
        # hash and append an "o".
        object_id = "o" + sha256((content_hash + schema_hash).encode("ascii")).hexdigest()[:-2]

        with self.object_engine.savepoint("object_rename"):

            self.object_engine.run_sql(
                SQL("ALTER TABLE {}.{} ADD COLUMN {} BOOLEAN DEFAULT TRUE").format(
                    Identifier(SPLITGRAPH_META_SCHEMA),
                    Identifier(tmp_object_id),
                    Identifier(SG_UD_FLAG),
                )
            )

            try:
                self.object_engine.store_object(
                    object_id=object_id,
                    source_schema=SPLITGRAPH_META_SCHEMA,
                    source_table=tmp_object_id,
                )
            except DuplicateTable:
                # If we already have an object with this ID (and hence hash), reuse it.
                logging.info(
                    "Reusing object %s for table %s/%s limit %r after_pk %r",
                    object_id,
                    source_schema,
                    source_table,
                    limit,
                    after_pk,
                )
                self.object_engine.delete_table(SPLITGRAPH_META_SCHEMA, tmp_object_id)

        with self.metadata_engine.savepoint("object_register"):
            try:
                self._register_object(
                    object_id,
                    namespace=namespace,
                    insertion_hash=content_hash,
                    deletion_hash="0" * 64,
                    parent_object=None,
                )
            except UniqueViolation:
                # Someone registered this object (perhaps a concurrent pull) already.
                logging.info(
                    "Object %s for table %s/%s limit %r after_pk %r already exists, continuing...",
                    object_id,
                    source_schema,
                    source_table,
                    limit,
                    after_pk,
                )

        return object_id

    def record_table_as_base(
        self,
        repository,
        table_name,
        image_hash,
        chunk_size=10000,
        source_schema=None,
        source_table=None,
    ):
        """
        Copies the full table verbatim into one or more new base fragments and registers them.

        :param repository: Repository
        :param table_name: Table name
        :param image_hash: Hash of the new image
        :param chunk_size: If specified, splits the table into multiple objects with a given number of rows
        :param source_schema: Override the schema the source table is stored in
        :param source_table: Override the name of the table the source is stored in
        """
        object_ids = []
        source_schema = source_schema or repository.to_schema()
        source_table = source_table or table_name

        table_size = self.object_engine.run_sql(
            SQL("SELECT COUNT (1) FROM {}.{}").format(
                Identifier(source_schema), Identifier(source_table)
            ),
            return_shape=ResultShape.ONE_ONE,
        )

        if chunk_size and table_size:
            table_pk = [
                p[0] for p in self.object_engine.get_primary_keys(source_schema, source_table)
            ]
            new_fragment = None

            for _ in range(0, table_size, chunk_size):
                # Chunk the table up. It's very slow to do it with
                # SELECT * FROM source LIMIT chunk_size OFFSET offset as Postgres
                # needs to go through `offset` heap fetches before finding out what it is it's
                # supposed to copy. So for the full table, PG will do 0 + chunk_size + 2 * chunk_size
                # + ... + (no_chunks - 1) * chunk_size heap fetches which is O(n^2).

                # Instead, we look at the last PK of the chunk we wrote previously and
                # copy rows starting from after that PK (which PG can locate with an index-only scan).

                # Technically we should see if we tried to write an empty chunk and then
                # stop chunking the table but since we know the exact table size, we know
                # exactly how many times to keep going.

                last_pk = None
                if new_fragment:
                    _, last_pk = self.extract_min_max_pks([new_fragment], table_pk)[0]

                new_fragment = self.create_base_fragment(
                    source_schema,
                    source_table,
                    repository.namespace,
                    limit=chunk_size,
                    after_pk=last_pk,
                )
                object_ids.append(new_fragment)

        elif table_size:
            object_ids.append(
                self.create_base_fragment(source_schema, source_table, repository.namespace)
            )
        # If table_size == 0, then we don't link it to any objects and simply store its schema
        table_schema = self.object_engine.get_full_table_schema(source_schema, source_table)
        self.register_tables(repository, [(image_hash, table_name, table_schema, object_ids)])

    def get_all_required_objects(self, object_ids):
        """
        Follow the parent chains of multiple objects until the base objects are reached.
        :param object_ids: Object IDs to start the traversal on.
        :return: Expanded chain. Parents of objects are guaranteed to come before those objects and
            the order in the `object_ids` array is preserved.
        """
        original_order = {o: i for i, o in enumerate(object_ids)}

        parents = self.metadata_engine.run_sql(
            SQL("SELECT object_id, original_object_id from {}.get_object_path(%s)").format(
                Identifier(SPLITGRAPH_API_SCHEMA)
            ),
            (object_ids,),
        )

        # Sort the resultant array so that objects that earlier items in the
        # `object_ids` list depend on come earlier themselves.
        return [
            object_id
            for object_id, _ in sorted(
                parents, key=lambda object_reason: original_order[object_reason[1]]
            )
        ]

    def filter_fragments(self, object_ids, quals, column_types):
        """
        Performs fuzzy filtering on the given object IDs using the index and a set of qualifiers, discarding
        objects that definitely do not match the qualifiers.

        :param object_ids: List of object IDs to filter.
        :param quals: List of qualifiers in conjunctive normal form that will be matched against the index.
            Objects that definitely don't match these qualifiers will be discarded.

            A list containing `[[qual_1, qual_2], [qual_3, qual_4]]` will be interpreted as
            (qual_1 OR qual_2) AND (qual_3 OR qual_4).

            Each qual is a tuple of `(column_name, operator, value)` where
            `operator` can be one of `>`, `>=`, `<`, `<=`, `=`.

            For unknown operators, it will be assumed that all fragments might match that clause.
        :param column_types: A dictionary mapping column names to their types.
        :return: List of objects that might match the given qualifiers.
        """
        # We need access to column types here since the objects might not have been fetched yet and so
        # we can't look at their column types.
        clause, args = _quals_to_clause(quals, column_types)

        # If we don't include the removed items (e.g. the previous values of the deleted/updated rows)
        # in the index, we have to instead fetch the whole chain starting from the first
        # pertinent object

        query = (
            select("get_object_meta", "object_id", table_args="(%s)", schema=SPLITGRAPH_API_SCHEMA)
            + SQL(" WHERE ")
            + clause
        )
        return self.metadata_engine.run_sql(
            query, [object_ids] + list(args), return_shape=ResultShape.MANY_ONE
        )


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
    """Generate a random ID for temporary/staging objects that haven't had their ID calculated yet.
    Note that Postgres limits table names to 63 characters, so the IDs shall be 248-bit strings, hex-encoded,
    + a letter prefix since Postgres doesn't seem to support table names starting with a digit."""
    # Make sure we're padded to 62 characters (otherwise if the random number generated is less than 2^247 we'll be
    # dropping characters from the hex format)
    return str.format("o{:062x}", getrandbits(248))


def _qual_to_index_clause(qual, ctype):
    """Convert our internal qual format into a WHERE clause that runs against an object's index entry.
    Returns a Postgres clause (as a Composable) and a tuple of arguments to be mogrified into it."""
    column_name, qual_op, value = qual

    # Our index is essentially a bloom filter: it returns True if an object _might_ have rows
    # that affect the result of a query with a given qual and False if it definitely doesn't.
    # Hence, we can combine qualifiers in a similar Boolean way (proof?)

    # If there's no index information for a given column, we have to assume it might match the qual.
    query = SQL("NOT index ? %s OR ")
    args = [column_name]

    # If the column has to be greater than (or equal to) X, it only might exist in objects
    # whose maximum value is greater than (or equal to) X.
    if qual_op in (">", ">="):
        query += SQL("(index #>> '{{{},1}}')::" + ctype + "  " + qual_op + " %s").format(
            (Identifier(column_name))
        )
        args.append(value)
    # Similar for smaller than, but here we check that the minimum value is smaller than X.
    elif qual_op in ("<", "<="):
        query += SQL("(index #>> '{{{},0}}')::" + ctype + " " + qual_op + " %s").format(
            (Identifier(column_name))
        )
        args.append(value)
    elif qual_op == "=":
        query += SQL(
            "%s BETWEEN (index #>> '{{{0},0}}')::"
            + ctype
            + " AND (index #>> '{{{0},1}}')::"
            + ctype
        ).format((Identifier(column_name)))
        args.append(value)
    # Currently, we ignore the LIKE (~~) qualifier since we can only make a judgement when the % pattern is at
    # the end of a string.
    # For inequality, we can't really say when an object is definitely not pertinent to a qual:
    #   * if a <> X and X is included in an object's range, the object still might have values that aren't X.
    #   * if X isn't included in an object's range, the object definitely has values that aren't X so we have
    #     to fetch it.
    else:
        # For all other operators, we don't know if they will match so we assume that they will.
        return SQL("TRUE"), ()
    return query, tuple(args)


def _qual_to_sql_clause(qual, ctype):
    """Convert a qual to a normal SQL clause that can be run against the actual object rather than the index."""
    column_name, qual_op, value = qual
    return SQL("{}::" + ctype + " " + qual_op + " %s").format(Identifier(column_name)), (value,)


def _quals_to_clause(quals, column_types, qual_to_clause=_qual_to_index_clause):
    if not quals:
        return SQL(""), ()

    def _internal_quals_to_clause(or_quals):
        clauses, args = zip(*[qual_to_clause(q, column_types[q[0]]) for q in or_quals])
        return (
            SQL(" OR ").join(SQL("(") + c + SQL(")") for c in clauses),
            tuple([a for arg in args for a in arg]),
        )

    clauses, args = zip(*[_internal_quals_to_clause(q) for q in quals])
    return (
        SQL(" AND ").join(SQL("(") + c + SQL(")") for c in clauses),
        tuple([a for arg in args for a in arg]),
    )


def quals_to_sql(quals, column_types):
    """
    Convert a list of qualifiers in CNF to a fragment of a Postgres query
    :param quals: Qualifiers in CNF
    :param column_types: Dictionary of column names and their types
    :return: SQL Composable object and a tuple of arguments to be mogrified into it.
    """

    return _quals_to_clause(quals, column_types, qual_to_clause=_qual_to_sql_clause)
