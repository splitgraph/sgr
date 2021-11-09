"""
Routines related to storing tables as fragments.
"""

import bisect
import itertools
import json
import logging
import operator
import struct
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import reduce
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Union, cast

from psycopg2._json import Json
from psycopg2.errors import UniqueViolation
from psycopg2.sql import SQL, Composable, Identifier
from splitgraph.config import CONFIG, SG_CMD_ASCII, SPLITGRAPH_API_SCHEMA, get_singleton
from splitgraph.core.indexing.bloom import filter_bloom_index, generate_bloom_index
from splitgraph.core.indexing.range import filter_range_index, generate_range_index
from splitgraph.core.metadata_manager import MetadataManager, Object
from splitgraph.core.types import Changeset, TableSchema
from splitgraph.engine import ResultShape
from splitgraph.engine.postgres.engine import (
    SG_UD_FLAG,
    add_ud_flag_column,
    chunk,
    get_change_key,
)
from splitgraph.exceptions import SplitGraphError
from tqdm import tqdm

from .common import SPLITGRAPH_META_SCHEMA, adapt, get_temporary_table_id
from .sql import select

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository
    from splitgraph.core.table import Table
    from splitgraph.engine.postgres.engine import PostgresEngine


def _split_changeset(
    changeset: Changeset, min_max: List[Tuple[Any, Any]], table_pks: List[Tuple[str, str]]
) -> Tuple[List[Changeset], Changeset, Changeset]:
    # maybe order min/max here
    maxs = [m[1] for m in min_max]
    changesets_by_segment: List[Changeset] = [{} for _ in range(len(min_max))]
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


def _key(c):
    # Hack to work around Nones sometimes being there in comparables. We make a fake
    # key, prepending a boolean (is the item None?) to every item to get a consistent
    # ordering that includes Nones.
    if isinstance(c, tuple):
        return tuple((ci is None, ci) for ci in c)
    return c is None, c


def _pk_overlap(pk_1: Tuple, pk_2: Tuple) -> bool:
    return bool(_key(pk_1[0]) <= _key(pk_2[1]) and _key(pk_1[1]) >= _key(pk_2[0]))


def get_chunk_groups(
    chunks: List[Tuple[str, Any, Any]],
) -> List[List[Tuple[str, Any, Any]]]:
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

    # no tuple concatenation (typechecker complains)

    chunks: List[Tuple[int, str, Any, Any]] = [
        (i, chunk[0], chunk[1], chunk[2]) for i, chunk in enumerate(chunks)
    ]
    groups: List[List[Tuple[int, str, Any, Any]]] = []
    current_group: List[Tuple[int, str, Any, Any]] = []
    current_group_start = None
    current_group_end = None

    for original_id, chunk_id, start, end in sorted(
        chunks, key=lambda c: _key(c[2])  # type:ignore
    ):
        if not current_group:
            current_group = [(original_id, chunk_id, start, end)]
            current_group_start = start
            current_group_end = end
            continue

        assert current_group_start
        assert current_group_end
        # See if the chunk overlaps with the current chunk group
        if _pk_overlap((start, end), (current_group_start, current_group_end)):
            current_group.append((original_id, chunk_id, start, end))
            current_group_start = min(current_group_start, start, key=_key)
            current_group_end = max(current_group_end, end, key=_key)
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

    def __init__(self, shorts: Tuple[int, ...]) -> None:
        """
        Create a Digest instance.

        :param shorts: Tuple of 16 2-byte integers.
        """
        # Shorts: tuple of 16 2-byte integers
        assert isinstance(shorts, tuple)
        assert len(shorts) == 16
        self.shorts = shorts

    @classmethod
    def empty(cls) -> "Digest":
        """Return an empty Digest instance such that for any Digest D, D + empty == D - empty == D"""
        return cls((0,) * 16)

    @classmethod
    def from_memoryview(cls, memory: Union[bytes, memoryview]) -> "Digest":
        """Create a Digest from a 256-bit memoryview/bytearray."""
        # Unpack the buffer as 16 signed big-endian shortints.
        return cls(struct.unpack(">16H", memory))

    @classmethod
    def from_hex(cls, hex_string: str) -> "Digest":
        """Create a Digest from a 64-characters (256-bit) hexadecimal string"""
        assert len(hex_string) == 64
        return cls(tuple(int(hex_string[i : i + 4], base=16) for i in range(0, 64, 4)))

    # In these routines, we treat each hash as a vector of 16 2-byte integers and do component-wise addition.
    # To simulate the wraparound behaviour of C shorts, throw away all remaining bits after the action.
    def __add__(self, other: "Digest") -> "Digest":
        return Digest(
            tuple((left + right) & 0xFFFF for left, right in zip(self.shorts, other.shorts))
        )

    def __sub__(self, other: "Digest") -> "Digest":
        return Digest(
            tuple((left - right) & 0xFFFF for left, right in zip(self.shorts, other.shorts))
        )

    def __neg__(self) -> "Digest":
        return Digest(tuple(-v & 0xFFFF for v in self.shorts))

    def hex(self) -> str:
        """Convert the hash into a hexadecimal value."""
        return struct.pack(">16H", *self.shorts).hex()


"""Dictionary of {index_type: [column: index_specific_kwargs or list of columns]}."""
ExtraIndexInfo = Dict[str, Union[List[str], Dict[str, Dict[str, Any]]]]


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

    def __init__(
        self, object_engine: "PostgresEngine", metadata_engine: Optional["PostgresEngine"] = None
    ) -> None:
        metadata_engine = metadata_engine or object_engine
        super().__init__(metadata_engine)
        self.object_engine = object_engine

    def generate_object_index(
        self,
        object_id: str,
        table_schema: TableSchema,
        changeset: Optional[Changeset] = None,
        extra_indexes: Optional[ExtraIndexInfo] = None,
    ) -> Dict[str, Any]:
        """
        Queries the max/min values of a given fragment for each column, used to speed up querying.

        :param object_id: ID of an object
        :param table_schema: Schema of the table the object belongs to.
        :param changeset: Optional, if specified, the old row values are included in the index.
        :param extra_indexes: Dictionary of {index_type: column: index_specific_kwargs}.
        :return: Dict containing the object index.
        """
        extra_indexes: ExtraIndexInfo = extra_indexes or {}

        # Default None, meaning run range index on all columns.
        range_index_columns: Optional[List[str]]
        try:
            range_index_columns = list(extra_indexes["range"])
        except KeyError:
            range_index_columns = None
        range_index: Dict[str, Any] = generate_range_index(
            self.object_engine, object_id, table_schema, changeset, columns=range_index_columns
        )
        indexes = {"range": range_index}

        # Process extra indexes
        for index_name, index_cols in extra_indexes.items():
            if index_name == "range":
                continue
            if index_name != "bloom":
                raise ValueError("Unsupported index type %s!" % index_name)
            if isinstance(index_cols, list):
                raise ValueError(
                    "Unexpected options for index 'bloom': "
                    "got list, expected dictionary {column: {probability/size: ...}}!"
                )

            index_dict = {}
            for index_col, index_kwargs in index_cols.items():
                logging.debug(
                    "Running index %s on column %s with parameters %r",
                    index_name,
                    index_col,
                    index_kwargs,
                )
                index_dict[index_col] = generate_bloom_index(
                    self.object_engine, object_id, changeset, index_col, **index_kwargs
                )
            indexes[index_name] = index_dict

        return indexes

    def _register_object(
        self,
        object_id: str,
        namespace: str,
        insertion_hash: str,
        deletion_hash: str,
        table_schema: TableSchema,
        rows_inserted: int,
        rows_deleted: int,
        changeset: Optional[Changeset] = None,
        extra_indexes: Optional[ExtraIndexInfo] = None,
    ) -> None:
        """
        Registers a Splitgraph object in the object tree and indexes it

        :param object_id: Object ID
        :param namespace: Namespace that owns the object. In registry mode, only namespace owners can alter or delete
            objects.
        :param insertion_hash: Homomorphic hash of all rows inserted by this fragment
        :param deletion_hash: Homomorphic hash of the old values of all rows deleted by this fragment
        :param table_schema: List of (ordinal, name, type, is_pk) with the schema of the table that this object
            belongs to.
        :param changeset: For patches, changeset that produced this object. Must be a dictionary of
            {PK: (True for upserted/False for deleted, old row (if updated or deleted))}. The old values
            are used to generate the min/max index for an object to know if it removes/updates some rows
            that might be pertinent to a query.
        :param extra_indexes: Dictionary of {index_type: column: index_specific_kwargs}.
        """
        object_size = self.object_engine.get_object_size(object_id)
        object_index = self.generate_object_index(object_id, table_schema, changeset, extra_indexes)
        self.register_objects(
            [
                Object(
                    object_id=object_id,
                    format="FRAG",
                    namespace=namespace,
                    size=object_size,
                    created=datetime.utcnow(),
                    insertion_hash=insertion_hash,
                    deletion_hash=deletion_hash,
                    object_index=object_index,
                    rows_inserted=rows_inserted,
                    rows_deleted=rows_deleted,
                )
            ]
        )

    @staticmethod
    def _extract_deleted_rows(changeset: Any, table_schema: TableSchema) -> Any:
        change_key = get_change_key(table_schema)
        pk_cols, _ = zip(*change_key)
        rows = []
        for pk, data in changeset.items():
            if not data[1]:
                # No old row: new value has been inserted.
                continue
            # Turn the changeset into an actual row in the correct order
            pk_index = 0
            row = []
            for col in sorted(table_schema):
                if col.name not in pk_cols:
                    row.append(data[1][col.name])
                else:
                    row.append(pk[pk_index])
                    pk_index += 1
            rows.append(row)
        return rows

    def _hash_old_changeset_values(
        self, changeset: Any, table_schema: TableSchema
    ) -> Tuple[Digest, int]:
        """
        Since we're not storing the values of the deleted rows (and we don't have access to them in the staging table
        because they've been deleted), we have to hash them in Python. This involves mimicking the return value of
        `SELECT t::text FROM table t`.

        :param changeset: Map PK -> (upserted/deleted, Map col -> old val)
        :param table_schema: Table schema
        :return: `Digest` object and the number of deleted rows.
        """
        rows = self._extract_deleted_rows(changeset, table_schema)
        if not rows:
            return Digest.empty(), 0

        # Horror alert: we hash newly created tables by essentially calling digest(row::text) in Postgres and
        # we don't really know how it turns some types to strings. So instead we give Postgres all of its deleted
        # rows back and ask it to hash them for us in the same way.
        # TODO we do not quote pg_type here
        inner_tuple = "(" + ",".join("%s::" + c.pg_type for c in table_schema) + ")"
        query = (  # nosec
            "SELECT digest(o::text, 'sha256') FROM (VALUES "
            + ",".join(itertools.repeat(inner_tuple, len(rows)))
            + ") o"
        )

        # By default (e.g. for changesets where nothing was deleted) we use a 0 hash (since adding it to any other
        # hash has no effect).
        digests = self.object_engine.run_sql(
            query,
            [o if not isinstance(o, dict) else Json(o) for row in rows for o in row],
            return_shape=ResultShape.MANY_ONE,
        )
        return (
            reduce(operator.add, map(Digest.from_memoryview, digests), Digest.empty()),
            len(digests),
        )

    def _store_changesets(
        self,
        table: "Table",
        changesets: Any,
        schema: str,
        extra_indexes: Optional[ExtraIndexInfo] = None,
        in_fragment_order: Optional[List[str]] = None,
        overwrite: bool = False,
        table_name: Optional[str] = None,
    ) -> List[str]:
        """
        Store and register multiple changesets as fragments.

        :param table: Table object the changesets belong to
        :param changesets: List of changeset dictionaries. Empty changesets will be ignored.
        :param schema: Schema the table is checked out into.
        :param extra_indexes: Dictionary of {index_type: column: index_specific_kwargs}.
        :param overwrite: Overwrite object if already exists.
        :return: List of created object IDs.
        """
        object_ids = []
        logging.info("Storing and indexing table %s", table.table_name)
        table_name = table_name or table.table_name
        for sub_changeset in tqdm(
            changesets, unit="objs", ascii=SG_CMD_ASCII, disable=len(changesets) < 3
        ):
            if not sub_changeset:
                continue
            # Store the fragment in a temporary location and then find out its hash and rename to the actual target.
            # Optimisation: in the future, we can hash the upserted rows that we need preemptively and possibly
            # avoid storing the object altogether if it's a duplicate.
            tmp_object_id = self._store_changeset(
                sub_changeset, table_name, schema, table.table_schema
            )

            (
                deletion_hash,
                insertion_hash,
                object_id,
                rows_inserted,
                rows_deleted,
            ) = self._get_patch_fragment_hashes_stats(sub_changeset, table, tmp_object_id)

            object_ids.append(object_id)

            # Wrap this rename in a SAVEPOINT so that if the table already exists,
            # the error doesn't roll back the whole transaction (us creating and registering all other objects).
            with self.object_engine.savepoint("object_rename"):
                source_query = SQL("SELECT * FROM {}.{}").format(
                    Identifier("pg_temp"), Identifier(tmp_object_id)
                )

                if in_fragment_order:
                    source_query += SQL(" ") + self._get_order_by_clause(
                        in_fragment_order, table.table_schema
                    )

                try:
                    self.object_engine.store_object(
                        object_id=object_id,
                        source_query=source_query,
                        schema_spec=add_ud_flag_column(table.table_schema),
                        overwrite=overwrite,
                    )
                except UniqueViolation:
                    # Someone registered this object (perhaps a concurrent pull) already.
                    logging.info(
                        "Object %s for table %s/%s already exists, continuing...",
                        object_id,
                        table.repository,
                        table.table_name,
                    )
                self.object_engine.delete_table("pg_temp", tmp_object_id)
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
                        table_schema=table.table_schema,
                        changeset=sub_changeset,
                        extra_indexes=extra_indexes,
                        rows_inserted=rows_inserted,
                        rows_deleted=rows_deleted,
                    )
                except UniqueViolation:
                    logging.info(
                        "Object %s for table %s/%s already exists, continuing...",
                        object_id,
                        table.repository,
                        table.table_name,
                    )

        return object_ids

    def _get_patch_fragment_hashes_stats(
        self, sub_changeset: Any, table: "Table", tmp_object_id: str
    ) -> Tuple[Digest, Digest, str, int, int]:
        # Digest the rows.
        deletion_hash, rows_deleted = self._hash_old_changeset_values(
            sub_changeset, table.table_schema
        )
        insertion_hash, rows_inserted = self.calculate_fragment_insertion_hash_stats(
            "pg_temp", tmp_object_id, table.table_schema
        )
        content_hash = (insertion_hash - deletion_hash).hex()
        schema_hash = self._calculate_schema_hash(table.table_schema)
        object_id = "o" + sha256((content_hash + schema_hash).encode("ascii")).hexdigest()[:-2]
        return deletion_hash, insertion_hash, object_id, rows_inserted, rows_deleted

    def _store_changeset(
        self, sub_changeset: Any, table: str, schema: str, table_schema: TableSchema
    ) -> str:
        tmp_object_id = get_temporary_table_id()
        upserted = [pk for pk, data in sub_changeset.items() if data[0]]
        deleted = [pk for pk, data in sub_changeset.items() if not data[0]]
        self.object_engine.store_fragment(
            upserted,
            deleted,
            "pg_temp",
            tmp_object_id,
            schema,
            table,
            table_schema,
        )
        return tmp_object_id

    def calculate_fragment_insertion_hash_stats(
        self, schema: str, table: str, table_schema: Optional[TableSchema] = None
    ) -> Tuple[Digest, int]:
        """
        Calculate the homomorphic hash of just the rows that a given fragment inserts
        :param schema: Schema the fragment is stored in.
        :param table: Name of the table the fragment is stored in.
        :return: A `Digest` object and the number of inserted rows
        """
        table_schema = table_schema or self.object_engine.get_full_table_schema(schema, table)
        columns_sql = SQL(",").join(
            SQL("o.") + Identifier(c.name) for c in table_schema if c.name != SG_UD_FLAG
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
        return insertion_hash, len(row_digests)

    def record_table_as_patch(
        self,
        old_table: "Table",
        schema: str,
        image_hash: str,
        new_schema_spec: TableSchema = None,
        split_changeset: bool = False,
        extra_indexes: Optional[ExtraIndexInfo] = None,
        in_fragment_order: Optional[List[str]] = None,
        overwrite: bool = False,
    ) -> None:
        """
        Flushes the pending changes from the audit table for a given table and records them,
        registering the new objects.

        :param old_table: Table object pointing to the current HEAD table
        :param schema: Schema the table is checked out into.
        :param image_hash: Image hash to store the table under
        :param new_schema_spec: New schema of the table (use the old table's schema by default).
        :param split_changeset: See `Repository.commit` for reference
        :param extra_indexes: Dictionary of {index_type: column: index_specific_kwargs}.
        """

        # TODO does the reasoning in the docstring actually make sense? If the point is to, say, for a query
        # (pk=5000) fetch 0 fragments instead of 1 small one, is it worth the cost of having 2 extra fragments?
        # If a changeset is really large (e.g. update the first 1000 rows, update the last 1000 rows) then sure,
        # this will help (for a query pk=5000 we don't need to fetch a 2000-row fragment) but maybe at that point
        # it's time to rewrite the table altogether?

        # Accumulate the diff in-memory. This might become a bottleneck in the future.
        changeset: Changeset = {}
        _conflate_changes(
            changeset,
            cast(
                List[Tuple[Tuple[str, ...], bool, Dict[str, Any], Dict[str, Any]]],
                self.object_engine.get_pending_changes(schema, old_table.table_name),
            ),
        )
        self.object_engine.discard_pending_changes(schema, old_table.table_name)
        current_objects = old_table.objects

        new_schema_spec = new_schema_spec or old_table.table_schema
        if changeset:
            if split_changeset:
                logging.debug("Splitting changesets")
                # Reorganize the current table's fragments into non-overlapping groups
                # and split the changeset to make sure it doesn't span (and hence merge) them.
                table_pks = self.object_engine.get_change_key(schema, old_table.table_name)
                changesets = self.split_changeset_boundaries(changeset, table_pks, current_objects)
            else:
                changesets = [changeset]

            # Store the changesets and find out their object IDs.
            object_ids = self._store_changesets(
                old_table,
                changesets,
                schema,
                extra_indexes,
                in_fragment_order=in_fragment_order,
                overwrite=overwrite,
            )
            # Finally, link the table to the new set of objects.
            self.register_tables(
                old_table.repository,
                [
                    (
                        image_hash,
                        old_table.table_name,
                        new_schema_spec,
                        old_table.objects + object_ids,
                    )
                ],
            )
        else:
            # Changes in the audit log cancelled each other out. Point the image to the same old objects.
            self.register_tables(
                old_table.repository,
                [(image_hash, old_table.table_name, new_schema_spec, old_table.objects)],
            )

    def split_changeset_boundaries(self, changeset, change_key, objects):
        min_max = self.get_min_max_pks(objects, change_key)
        groups = get_chunk_groups([(o, mm[0], mm[1]) for o, mm in zip(objects, min_max)])
        group_boundaries = [
            (min(min_pk for _, min_pk, _ in group), max(max_pk for _, _, max_pk in group))
            for group in groups
        ]
        matched, before, after = _split_changeset(changeset, group_boundaries, change_key)
        changesets = [before] + matched + [after]
        return changesets

    def get_min_max_pks(
        self, fragments: List[str], table_pks: List[Tuple[str, str]]
    ) -> List[Tuple[Tuple, Tuple]]:
        """Get PK ranges for given fragments using the index (without reading the fragments).

        :param fragments: List of object IDs (must be registered and with the same schema)
        :param table_pks: List of tuples (column, type) that form the object PK.

        :return: List of (min, max) PK for every fragment where PK is a tuple.
            If a fragment doesn't exist or doesn't have a corresponding index entry,
            a SplitGraphError is raised.
        """
        # If the PK isn't composite, we can read the range for the corresponding column
        # from the index, otherwise, the indexer stored the min/max tuple under $pk.
        pk = table_pks[0][0] if len(table_pks) == 1 else "$pk"
        fields = SQL("object_id, index #>> '{{range,{0},0}}', index #>> '{{range,{0},1}}'").format(
            Identifier(pk)
        )

        result = {
            r[0]: (r[1], r[2])
            for r in self.metadata_engine.run_chunked_sql(
                select(
                    "get_object_meta",
                    fields.as_string(self.metadata_engine.connection),
                    table_args="(%s)",
                    schema=SPLITGRAPH_API_SCHEMA,
                ),
                (fragments,),
                chunk_position=0,
            )
        }

        # Since the PK can't contain a NULL, if we do get one here, it's from the JSON query
        # (column doesn't exist in the index).

        min_max = []
        for fragment in fragments:
            if fragment not in result:
                raise SplitGraphError("No metadata found for object %s!" % fragment)
            min_pk, max_pk = result[fragment]
            if min_pk is None or max_pk is None:
                raise SplitGraphError("No index found for object %s!" % fragment)
            if pk == "$pk":
                # For composite PKs, we're given back a JSON array and need to load it.
                min_pk = tuple(json.loads(min_pk))
                max_pk = tuple(json.loads(max_pk))
            else:
                # Single-column PKs still need to be returned as tuples.
                min_pk = (min_pk,)
                max_pk = (max_pk,)

            # Coerce the PKs to the actual Python types
            min_pk = tuple(adapt(v, c[1]) for v, c in zip(min_pk, table_pks))
            max_pk = tuple(adapt(v, c[1]) for v, c in zip(max_pk, table_pks))
            min_max.append((min_pk, max_pk))

        return min_max

    def calculate_content_hash(
        self,
        schema: str,
        table: str,
        table_schema: Optional[TableSchema] = None,
        chunk_condition_sql: Optional[Composable] = None,
        chunk_condition_args: Optional[List[Any]] = None,
    ) -> Tuple[str, int]:
        """
        Calculates the homomorphic hash of table contents.

        :param schema: Schema the table belongs to
        :param table: Name of the table
        :param table_schema: Schema of the table
        :param chunk_condition_sql: Column the table is partitioned on
        :param chunk_condition_args: Column value to get rows from
        :return: A 64-character (256-bit) hexadecimal string with the content hash of the table
            and the number of rows in the hash.
        """
        table_schema = table_schema or self.object_engine.get_full_table_schema(schema, table)
        digest_query = (
            SQL("SELECT digest((")
            + SQL(",").join(Identifier(c.name) for c in table_schema)
            + SQL(")::text, 'sha256'::text) FROM {}.{} o").format(
                Identifier(schema), Identifier(table)
            )
        )
        args = None
        if chunk_condition_sql:
            digest_query += SQL(" ") + chunk_condition_sql
            args = chunk_condition_args

        row_digests = self.object_engine.run_sql(
            digest_query, args, return_shape=ResultShape.MANY_ONE
        )

        return (
            reduce(operator.add, (Digest.from_memoryview(r) for r in row_digests)).hex(),
            len(row_digests),
        )

    def create_base_fragment(
        self,
        source_schema: str,
        source_table: str,
        namespace: str,
        chunk_condition_sql: Optional[Composable] = None,
        chunk_condition_args: Optional[List[Any]] = None,
        extra_indexes: Optional[ExtraIndexInfo] = None,
        in_fragment_order: Optional[List[str]] = None,
        overwrite: bool = False,
        table_schema: Optional[TableSchema] = None,
    ) -> str:
        if source_schema == "pg_temp" and not table_schema:
            raise ValueError("Cannot infer the schema of temporary tables, pass in table_schema!")

        # Fragments can't be reused in tables with different schemas
        # even if the contents match (e.g. '1' vs 1). Hence, include the table schema
        # in the object ID as well.
        table_schema = table_schema or self.object_engine.get_full_table_schema(
            source_schema, source_table
        )

        schema_hash = self._calculate_schema_hash(table_schema)
        # Get content hash for this chunk.
        content_hash, rows_inserted = self.calculate_content_hash(
            source_schema,
            source_table,
            table_schema,
            chunk_condition_sql=chunk_condition_sql,
            chunk_condition_args=chunk_condition_args,
        )

        # Object IDs are also used to key tables in Postgres so they can't be more than 63 characters.
        # In addition, table names can't start with a number (they can but every invocation has to
        # be quoted) so we have to drop 2 characters from the 64-character hash and append an "o".
        object_id = "o" + sha256((content_hash + schema_hash).encode("ascii")).hexdigest()[:-2]

        with self.object_engine.savepoint("object_rename"):
            # Store the object adding the extra update/delete column (always True in this case
            # since we don't overwrite any rows) and filtering on the chunk ID.

            source_query = (
                SQL("SELECT ")
                + SQL(",").join(Identifier(c.name) for c in table_schema)
                + SQL(",TRUE AS ")
                + Identifier(SG_UD_FLAG)
                + SQL("FROM {}.{}").format(Identifier(source_schema), Identifier(source_table))
            )
            source_query_args = None

            if chunk_condition_sql:
                source_query += SQL(" ") + chunk_condition_sql
                source_query_args = chunk_condition_args

            if in_fragment_order:
                source_query += SQL(" ") + self._get_order_by_clause(
                    in_fragment_order, table_schema
                )
            try:
                self.object_engine.store_object(
                    object_id=object_id,
                    source_query=source_query,
                    schema_spec=add_ud_flag_column(table_schema),
                    source_query_args=source_query_args,
                    overwrite=overwrite,
                )
            except UniqueViolation:
                # Someone registered this object (perhaps a concurrent pull) already.
                logging.info(
                    "Object %s for table %s/%s already exists, continuing...",
                    object_id,
                    source_schema,
                    source_table,
                )
        with self.metadata_engine.savepoint("object_register"):
            try:
                self._register_object(
                    object_id,
                    namespace=namespace,
                    insertion_hash=content_hash,
                    deletion_hash="0" * 64,
                    table_schema=table_schema,
                    extra_indexes=extra_indexes,
                    rows_inserted=rows_inserted,
                    rows_deleted=0,
                )
            except UniqueViolation:
                # Someone registered this object (perhaps a concurrent pull) already.
                logging.info(
                    "Object %s for table %s/%s already exists, continuing...",
                    object_id,
                    source_schema,
                    source_table,
                )

        return object_id

    @staticmethod
    def _get_order_by_clause(in_fragment_order, table_schema):
        column_names = [s.name for s in table_schema]
        if in_fragment_order:
            for c in in_fragment_order:
                if c not in column_names:
                    raise ValueError("Unknown column name %s, can't sort by it!" % c)
        return SQL("ORDER BY ") + SQL(",").join(Identifier(c) for c in in_fragment_order)

    @staticmethod
    def _calculate_schema_hash(table_schema):
        # Don't include column comments in the schema hash.
        return sha256(
            str([(c.ordinal, c.name, c.pg_type, c.is_pk) for c in table_schema]).encode("utf-8")
        ).hexdigest()

    def record_table_as_base(
        self,
        repository: "Repository",
        table_name: str,
        image_hash: str,
        chunk_size: Optional[int] = 10000,
        source_schema: Optional[str] = None,
        source_table: Optional[str] = None,
        extra_indexes: Optional[ExtraIndexInfo] = None,
        in_fragment_order: Optional[List[str]] = None,
        overwrite: bool = False,
        table_schema: Optional[TableSchema] = None,
    ) -> List[str]:
        """
        Copies the full table verbatim into one or more new base fragments and registers them.

        :param repository: Repository
        :param table_name: Table name
        :param image_hash: Hash of the new image
        :param chunk_size: If specified, splits the table into multiple objects with a given number of rows
        :param source_schema: Override the schema the source table is stored in
        :param source_table: Override the name of the table the source is stored in
        :param extra_indexes: Dictionary of {index_type: column: index_specific_kwargs}.
        :param in_fragment_order: Key to sort data inside each chunk by.
        :param overwrite: Overwrite physical objects that already exist.
        :param table_schema: Override the columns that will be picked from the original table
            (e.g. to change their order or primary keys). Note that the schema must be a subset
            of the original schema and this method doesn't verify PK constraints.
        """
        source_schema = source_schema or repository.to_schema()
        source_table = source_table or table_name

        table_not_empty = self.object_engine.run_sql(
            SQL("SELECT EXISTS(SELECT 1 FROM {}.{})").format(
                Identifier(source_schema), Identifier(source_table)
            ),
            return_shape=ResultShape.ONE_ONE,
        )

        table_schema = table_schema or self.object_engine.get_full_table_schema(
            source_schema, source_table
        )
        if chunk_size and table_not_empty:
            object_ids = self._chunk_table(
                repository,
                source_schema,
                source_table,
                chunk_size,
                extra_indexes,
                in_fragment_order=in_fragment_order,
                overwrite=overwrite,
                table_schema=table_schema,
            )

        elif table_not_empty:
            object_ids = [
                self.create_base_fragment(
                    source_schema,
                    source_table,
                    repository.namespace,
                    extra_indexes=extra_indexes,
                    in_fragment_order=in_fragment_order,
                    overwrite=overwrite,
                    table_schema=table_schema,
                )
            ]
        else:
            # If the table is empty, then we don't link it to any objects and simply store its schema
            object_ids = []
        self.register_tables(repository, [(image_hash, table_name, table_schema, object_ids)])
        return object_ids

    def _chunk_table(
        self,
        repository: "Repository",
        source_schema: str,
        source_table: str,
        chunk_size: int,
        extra_indexes: Optional[ExtraIndexInfo] = None,
        table_schema: Optional[TableSchema] = None,
        in_fragment_order: Optional[List[str]] = None,
        overwrite: bool = False,
    ) -> List[str]:
        table_schema = table_schema or self.object_engine.get_full_table_schema(
            source_schema, source_table
        )
        change_key = get_change_key(table_schema)
        table_pk = [p[0] for p in change_key]
        surrogate_pk = not any(t.is_pk for t in table_schema)

        # We need to do multiple things here in a specific way to not tank the performance:
        #  * Chunk the table up ordering by PK (or potentially another chunk key in the future)
        #  * Run LTHash on added rows in every chunk
        #  * Copy each chunk into a CStore table (adding ranges/bloom filter intex to it
        #    in the metadata).
        #
        # Chunking is very slow to do with
        #   SELECT * FROM source LIMIT chunk_size OFFSET offset ORDER BY pk
        # as Postgres needs (even if there's an index on the PK) to go through first `offset` tuples
        # before finding out what it is it's supposed to copy. So for the full table, PG will do
        # 0 + chunk_size + 2 * chunk_size + ... + (no_chunks - 1) * chunk_size fetches
        # which is O(n^2).
        #
        # The second strategy here was recording the last PK we saw and then copying the next
        # fragment out of the table with SELECT ... WHERE pk > last_pk ORDER BY pk LIMIT chunk_size
        # but that still led to poor performance on large tables (8M rows, chunks of ~200k rows
        # would take 1 minute each to create).
        #
        # Third attempt was adding a temporary column to the source table using RANK () OVER
        #   (ORDER BY pk) but that required a join with a CTE and a couple of sequential
        # scans which also took more than 15 minutes on a 8M row table, no matter whether the
        # table had indexes on the join key.
        #
        # Fourth attempt was: compute the partition key and extract the table contents
        # into a TEMPORARY table, then create an index on that partition key, then copy data
        # out of it into CStore. This still meant having to copy the table over twice.
        #
        # Current incarnation: create a temporary table with partition boundaries and then
        # use that as the chunking condition (lower <= pk < upper)
        logging.info("Processing table %s", source_table)
        temp_table = "_sg_tmp_partition_" + source_table

        pk_sql = SQL(",").join(Identifier(p) for p in table_pk)

        # If we don't have a PK, we cast the whole row to text when partitioning
        # the table as well as selecting from it (add an _sg_surrogate_pk col)
        cast_if_surrogate = SQL("::text" if surrogate_pk else "")
        # Compute the partition boundaries
        tmp_table_query = (
            SQL("CREATE TABLE {}.{} AS WITH _row_nums AS (SELECT").format(
                Identifier(source_schema), Identifier(temp_table)
            )
            + SQL("(ROW_NUMBER() OVER (ORDER BY (")
            + pk_sql
            + SQL(")")
            + cast_if_surrogate
            + SQL(") - 1) _sg_tmp_row_num, ")
            + (SQL("(") + pk_sql + SQL(")::text AS _sg_surrogate_pk") if surrogate_pk else pk_sql)
            + SQL(" FROM {}.{}").format(Identifier(source_schema), Identifier(source_table))
            + SQL(") SELECT ")
            + SQL("_sg_tmp_row_num / %s AS _sg_tmp_partition_id, ")
            + (SQL("_sg_surrogate_pk") if surrogate_pk else pk_sql)
            + SQL(" FROM _row_nums WHERE _sg_tmp_row_num %% %s = 0")
        )
        object_ids = []

        try:
            self.object_engine.run_sql(tmp_table_query, (chunk_size, chunk_size))

            all_chunks = self.object_engine.run_sql(
                SQL("SELECT * FROM {}.{} ORDER BY _sg_tmp_partition_id").format(
                    Identifier(source_schema), Identifier(temp_table)
                )
            )
            logging.debug("Chunk boundaries: %s", all_chunks)

            log_progress = len(all_chunks) > 10
            log_func = logging.info if log_progress else logging.debug

            log_func("Storing and indexing the table")

            worker_threads = int(get_singleton(CONFIG, "SG_ENGINE_POOL")) - 1

            def _store_object(chunk_id: int):
                # Build the condition for the chunk. For some reason, this:
                #
                #   WHERE [pk] >= (SELECT [pk] FROM [partition_table] WHERE partition_id = chunk_id)
                #
                # is 10x slower than plugging the PK in directly
                # https://stackoverflow.com/questions/14987321/postgresql-in-operator-with-subquery-poor-performance ?

                _pk_placeholder = (
                    ",".join(itertools.repeat("%s", len(table_pk))) if not surrogate_pk else "%s"
                )
                chunk_condition = (
                    SQL("WHERE (")
                    + pk_sql
                    + SQL(")")
                    + cast_if_surrogate
                    + SQL(" >= (" + _pk_placeholder + ")")
                )
                chunk_args = all_chunks[chunk_id][1:]
                if chunk_id + 1 < len(all_chunks):
                    chunk_condition += (
                        SQL(" AND (")
                        + pk_sql
                        + SQL(")")
                        + cast_if_surrogate
                        + SQL(" < (" + _pk_placeholder + ")")
                    )
                    chunk_args = chunk_args + all_chunks[chunk_id + 1][1:]
                logging.debug(
                    "Storing chunk %s. Condition: %s. Boundaries: %s",
                    chunk_id,
                    chunk_condition.as_string(self.object_engine.connection),
                    chunk_args,
                )
                new_fragment = self.create_base_fragment(
                    source_schema,
                    source_table,
                    repository.namespace,
                    chunk_condition_sql=chunk_condition,
                    chunk_condition_args=chunk_args,
                    extra_indexes=extra_indexes,
                    in_fragment_order=in_fragment_order,
                    overwrite=overwrite,
                    table_schema=table_schema,
                )
                self.object_engine.commit()
                self.metadata_engine.commit()
                return new_fragment

            with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
                pbar = tqdm(
                    tpe.map(_store_object, range(0, len(all_chunks))),
                    total=len(all_chunks),
                    unit="objs",
                    ascii=SG_CMD_ASCII,
                    disable=not log_progress,
                )
                for object_id in pbar:
                    object_ids.append(object_id)
                    pbar.set_postfix(object=object_id[:10] + "...")
        finally:
            self.object_engine.close_others()
            self.object_engine.delete_table(source_schema, temp_table)
        return object_ids

    def filter_fragments(self, object_ids: List[str], table: "Table", quals: Any) -> List[str]:
        """
        Performs fuzzy filtering on the given object IDs using the index and a set of qualifiers, discarding
        objects that definitely do not match the qualifiers.

        :param object_ids: List of object IDs to filter.
        :param table: A Table object the objects belong to.
        :param quals: List of qualifiers in conjunctive normal form that will be matched against the index.
            Objects that definitely don't match these qualifiers will be discarded.

            A list containing `[[qual_1, qual_2], [qual_3, qual_4]]` will be interpreted as
            (qual_1 OR qual_2) AND (qual_3 OR qual_4).

            Each qual is a tuple of `(column_name, operator, value)` where
            `operator` can be one of `>`, `>=`, `<`, `<=`, `=`.

            For unknown operators, it will be assumed that all fragments might match that clause.
        :return: List of objects that might match the given qualifiers.
        """
        if not quals:
            return object_ids

        column_types = {c[1]: c[2] for c in table.table_schema}

        # Run the range filter
        range_filter_result = filter_range_index(
            self.metadata_engine, object_ids, quals, column_types
        )
        if len(range_filter_result) < len(object_ids):
            logging.info(
                "Range filter discarded %d/%d fragment(s)",
                len(object_ids) - len(range_filter_result),
                len(object_ids),
            )

        # Run other filters: currently we can attempt to run the bloom filter
        # if the fragment metadata has bloom fingerprints.
        bloom_filter_result = filter_bloom_index(self.metadata_engine, range_filter_result, quals)
        if len(bloom_filter_result) < len(range_filter_result):
            logging.info(
                "Bloom filter discarded %d/%d fragment(s)",
                len(range_filter_result) - len(bloom_filter_result),
                len(range_filter_result),
            )

        objects_to_scan = self._add_overlapping_objects(table, object_ids, bloom_filter_result)

        if len(objects_to_scan) > len(bloom_filter_result):
            logging.info(
                "Will need to scan through extra %d overlapping fragment(s)",
                len(objects_to_scan) - len(bloom_filter_result),
            )

        # Preserve original object order.
        return [r for r in object_ids if r in objects_to_scan]

    def generate_surrogate_pk(
        self, table: "Table", object_pks: List[Tuple[Any, Any]]
    ) -> List[Tuple[Any, Any]]:
        """
        When partitioning data, if the table doesn't have a primary key, we use a "surrogate"
        primary key by concatenating the whole row as a string on the PG side (this is because
        the whole row can sometimes contain NULLs which we can't compare in PG).

        We need to mimic this when calculating if the objects we're about to scan through
        overlap: e.g. using string comparison, "(some_country, 100)" < "(some_country, 20)",
        whereas using typed comparison, (some_country, 100) > (some_country, 20).

        To do this, we use a similar hack from when calculating changeset hashes: to avoid having
        to reproduce how PG's ::text works, we give it back the rows and get it to cast them
        to text for us.
        """
        inner_tuple = (
            "(" + ",".join("%s::" + ct for _, ct in get_change_key(table.table_schema)) + ")"
        )
        rows = [r for o in object_pks for r in o]

        result = []
        for batch in chunk(rows, 1000):
            query = (  # nosec
                "SELECT o::text FROM (VALUES "
                + ",".join(itertools.repeat(inner_tuple, len(batch)))
                + ") o"
            )
            result.extend(
                self.object_engine.run_sql(
                    query,
                    [o if not isinstance(o, dict) else Json(o) for row in batch for o in row],
                    return_shape=ResultShape.MANY_ONE,
                )
            )
        object_pks = list(zip(result[::2], result[1::2]))
        return object_pks

    def _add_overlapping_objects(
        self, table: "Table", all_objects: List[str], filtered_objects: List[str]
    ) -> Set[str]:
        # Expand the list of objects by also adding the objects that might overwrite these. In some
        # cases, we don't keep track of the rows that an object deletes in the index, since that
        # adds an implicit dependency on those previous objects.

        table_pk = get_change_key(table.table_schema)
        object_pks = self.get_min_max_pks(all_objects, table_pk)

        surrogate_pk = not any(t.is_pk for t in table.table_schema)
        if surrogate_pk:
            object_pks = self.generate_surrogate_pk(table, object_pks)

        # Go through all objects and see if they 1) come after any of our chosen objects and 2)
        # overlap those objects' PKs (if they come after them)
        original_order = {object_id: i for i, object_id in enumerate(all_objects)}
        object_pk_dict = {
            object_id: object_pk for object_id, object_pk in zip(all_objects, object_pks)
        }
        objects_to_scan = set(filtered_objects)
        for overlap_candidate in all_objects:
            if overlap_candidate in objects_to_scan:
                continue
            for our_object in filtered_objects:
                if original_order[our_object] < original_order[overlap_candidate] and _pk_overlap(
                    object_pk_dict[our_object], object_pk_dict[overlap_candidate]
                ):
                    objects_to_scan.add(overlap_candidate)
                    break
        return objects_to_scan

    def delete_objects(self, objects: Union[Set[str], List[str]]) -> None:
        """
        Deletes objects from the Splitgraph cache

        :param objects: A sequence of objects to be deleted
        """
        objects = list(objects)
        for i in range(0, len(objects), 100):
            to_delete = objects[i : i + 100]
            table_types = self.object_engine.run_sql(
                SQL(  # nosec
                    "SELECT table_name, table_type FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name IN ("
                    + ",".join(itertools.repeat("%s", len(to_delete)))
                    + ")"
                ),
                [SPLITGRAPH_META_SCHEMA] + to_delete,
            )

            base_tables = [tn for tn, tt in table_types if tt == "BASE TABLE"]
            # Try deleting CStore-mounted objects regardless of whether they're
            # in splitgraph_meta as foreign tables (there might be cases
            # where they are in /var/lib/splitgraph/objects but not mounted)
            foreign_tables = [tn for tn in to_delete if tn not in base_tables]

            if base_tables:
                self.object_engine.run_sql(
                    SQL(";").join(
                        SQL("DROP TABLE IF EXISTS {}.{}").format(
                            Identifier(SPLITGRAPH_META_SCHEMA), Identifier(t)
                        )
                        for t in base_tables
                    )
                )
            if foreign_tables:
                self.object_engine.delete_objects(foreign_tables)

            self.object_engine.commit()


def _conflate_changes(
    changeset: Changeset, new_changes: List[Tuple[Tuple, bool, Dict[str, Any], Dict[str, Any]]]
) -> Changeset:
    """
    Updates a changeset to incorporate the new changes. Assumes that the new changes are non-pk changing
    (i.e. PK-changing updates have been converted into a del + ins).
    """
    for change_pk, upserted, old_row, new_row in new_changes:
        old_change = changeset.get(change_pk)
        if not old_change:
            changeset[change_pk] = (upserted, old_row, new_row)
        else:
            # If we reinserted the same row that we deleted or deleted a row that
            # was inserted, drop the change completely.
            if old_change[1] == new_row:
                del changeset[change_pk]
            else:
                changeset[change_pk] = (upserted, old_change[1], new_row)

    return changeset
