"""Bloom filtering on fragments for equality queries."""
import base64
import itertools
import struct
from datetime import datetime
from hashlib import sha256
from math import ceil, log, exp
from typing import Any, Dict, List, Optional, Tuple, Union, cast, TYPE_CHECKING

from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core.output import pretty_size
from splitgraph.core.types import Changeset
from splitgraph.engine.postgres.engine import SG_UD_FLAG

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PsycopgEngine


def _hash_value(value: Union[datetime, int, str, None]) -> Tuple[bytes, bytes]:
    if value is None:
        # See explanation in generate_bloom_index for why this isn't
        # a completely terrible idea.
        value = "NULL"
    return (
        sha256(str(value).encode("utf-8")).digest(),
        sha256((str(value) + "salt").encode("utf-8")).digest(),
    )


def generate_bloom_index(
    engine: "PsycopgEngine",
    object_id: str,
    changeset: Optional[Changeset],
    column: str,
    probability: Optional[float] = None,
    size: Optional[int] = None,
) -> Tuple[int, str]:
    """
    Generates a bloom filter signature for a given column and a given fragment. Bloom filters
    can answer queries asking whether an item is definitely not in a given set or possibly can be.

    The tradeoff is between the probability of a false positive (item said to be in the set when
    it actually isn't) and the size of the filter.

    Bloom filters also have an extra parameter, k, or the number of bits in the signature that
    a certain item flips. This parameter has an optimal value for a given number of distinct items
    or a probability and so isn't explicitly passed by the user.

    :param engine: Object engine the fragment is cached in.
    :param object_id: Fragment ID
    :param changeset: Optional, if specified, the old column values are included in the index.
    :param column: Column name to generate the index on.
    :param probability: Probability of a false positive. Either this or the size of the filter must
        be specified, but not both.
    :param size: Size of the filter, in bytes.
    :return: Dictionary to be inserted into the index.
    """

    if not (probability is None) ^ (size is None):
        raise ValueError("One of probability or size must be specified, but not both!")

    # We need k hash functions to generate a signature for every item, which we can construct
    # by taking a linear combination of two hash functions. The first hash function is a simple sha of the
    # column, the second one is a sha of the column + a deterministic salt.
    # First, we outsource the actual hashing to Postgres.

    # NULLs are interesting since a sha of a NULL is a NULL again. Here, we
    # turn them into strings with "NULL". This does mean they will collide if
    # we are indexing a string column with an actual "NULL" in it, but in this case
    # it will only mean chunks with NULLs will be fetched for a query with "NULL"
    # and vice versa, which doesn't break anything (this is just a preflight optimisation).

    digest_query = SQL(
        "SELECT digest(coalesce({0}::text, 'NULL'), 'sha256'), "
        "digest(coalesce({0}::text, 'NULL') || 'salt', 'sha256') "
        "FROM {1}.{2} o WHERE o.{3} = true"
    ).format(
        Identifier(column),
        Identifier(SPLITGRAPH_META_SCHEMA),
        Identifier(object_id),
        Identifier(SG_UD_FLAG),
    )

    digests = engine.run_sql(digest_query)

    # Add digests of the old values in the changeset for this column.
    if changeset:
        for _, old_row, _ in changeset.values():
            if column in old_row:
                # Note that this will add this value to the digest
                # even if it didn't get changed (say the row got overwritten
                # but another column got changed). This means that the value
                # in this case will be counted twice (both the new value
                # that we hashed before and the old value which we're
                # hashing now which is the same). This is not an issue
                # since we deduplicate our digests and the same digest
                # will set the same bits in the filter to 1, but something
                # to keep in mind.
                digests.append(_hash_value(old_row[column]))

    # Count the number of distinct items and determine the size (if needed) and optimal number
    # of hash functions.
    distinct_items = list(set(digests))

    if probability:
        # The formula gives the number of bits in the array, but we divide it by
        # 8 since we'll be using a byte array for operations + to store the signature.
        size = int(ceil(-len(distinct_items) * log(probability) / log(2) ** 2 / 8))

    # For mypy: we've asserted previously that either probability or size are specified
    # and we calculate the size from the probability, so size is no longer Optional[int].
    size = cast(int, size)

    size_bits = size * 8
    no_funcs = int(ceil(log(2) * size_bits / len(distinct_items)))

    # Generate the filter
    result = bytearray(size)
    for hash_1, hash_2 in distinct_items:
        hash_1 = int.from_bytes(hash_1, byteorder="big")
        hash_2 = int.from_bytes(hash_2, byteorder="big")
        for i in range(no_funcs):
            hash_i = (hash_1 + i * hash_2) % size_bits
            result[hash_i // 8] |= 1 << hash_i % 8

    return no_funcs, base64.b64encode(result).decode("ascii")


def describe(index_tuple: Tuple[int, str]) -> str:
    """
    Returns a pretty-printed summary of the bloom filter

    :param index_tuple: Tuple of (k, base64-encoded fingerprint)
        returned by generate_bloom_index
    :return: String
    """

    k, bloom_filter = index_tuple
    footprint = len(bloom_filter)
    bloom_filter = base64.b64decode(bloom_filter)

    # Get filter size in bytes (this is not the in-db footprint of the base64 value)
    filter_size = len(bloom_filter)

    # Calculate the number of set bits (used to approximate number of items)
    set_bits = 0
    for n in struct.unpack("B" * (len(bloom_filter)), bloom_filter):
        set_bits += bin(n).count("1")

    approx_items = -(filter_size * 8) / k * log(1 - set_bits / filter_size / 8)

    # Calculate the rough false positive probability
    probability = (1 - exp(-k * approx_items / filter_size / 8)) ** k

    return "k=%d, size %s, approx. %d item(s), false positive probability %.1f%%" % (
        k,
        pretty_size(footprint),
        int(approx_items),
        probability * 100,
    )


def _prepare_bloom_quals(quals: Any) -> List[List[Tuple[str, int, int]]]:
    """
    Convert list of qualifiers in CNF (ANDed OR-clauses where each clause is "column, operator, value")
    to prepare it for querying the bloom filter:

    * Clauses where operator isn't equality are set to True (we can't make a judgement on anything
      but exact matches).
    * Clauses with equality are converted to (column, hash_1, hash_2) so that the sought value
      isn't re-hashed for every fragment
    * OR-clauses where one operator is True are set to True completely (e.g. if we query
      a = 5 OR b > 6, the bloom filter can't say with certainty that there are no rows
      with b > 6 in the fragment and so we have to inspect it)
    * The toplevel AND-clause has all True values removed.

    :param quals: Quals in CNF.
    :return: Transformed list of quals
    """

    def _process_qual(qual):
        column, operator, value = qual
        if operator != "=":
            return True

        hash_1, hash_2 = _hash_value(value)
        hash_1 = int.from_bytes(hash_1, byteorder="big")
        hash_2 = int.from_bytes(hash_2, byteorder="big")

        return column, hash_1, hash_2

    def _process_or(quals):
        result = []
        for qual in quals:
            qual = _process_qual(qual)
            if qual is True:
                # anything OR True is True
                return True
            result.append(qual)
        return result

    result = []
    for or_quals in quals:
        or_quals = _process_or(or_quals)
        if or_quals is not True:
            result.append(or_quals)

    return result


def _match(qual: Tuple[str, int, int], bloom_index: Dict[str, Tuple[int, bytes]]) -> bool:
    """
    Checks whether a processed qual (column, hash_1, hash_2) can match a fragment with
    a given index.

    :param qual:
    :param bloom_index:
    """

    column, hash_1, hash_2 = qual
    if column not in bloom_index:
        # No index info for this column -- might match
        return True

    no_funcs, bloom_filter = bloom_index[column]
    size_bits = len(bloom_filter) * 8
    for i in range(no_funcs):
        hash_i = (hash_1 + i * hash_2) % size_bits
        if not bloom_filter[hash_i // 8] & (1 << hash_i % 8):
            # If at least one position in the filter isn't filled,
            # this qualifier can't be met by anything in the fragment.
            return False

    return True


def filter_bloom_index(engine: "PsycopgEngine", object_ids: List[str], quals: Any) -> List[str]:
    """
    Runs a bloom filter on given qualifiers using the given objects' previously-generated
    fingerprints.

    :param engine: Object engine
    :param object_ids: Object IDs
    :param quals: List of qualifiers
    :return: List of object IDs that might match the qualifiers in `quals` (including
        IDs that don't have a bloom index).
    """
    if not object_ids:
        return object_ids

    quals = _prepare_bloom_quals(quals)
    # If we don't have any equalities in quals or quals collapse to something
    # that the bloom filter can't make a judgement about, do nothing.
    if not quals:
        return object_ids

    # Load the index: my SQLfu isn't strong enough to create a query that takes
    # care of varying values of K and varying signature sizes.
    bloom_index = engine.run_sql(
        SQL(
            "SELECT object_id, index -> 'bloom' FROM {}.{} WHERE object_id IN ("
            + ",".join(itertools.repeat("%s", len(object_ids)))
            + ")"
        ).format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier("objects")),
        object_ids,
    )

    bloom_index = {
        o: {col: (i[0], base64.b64decode(i[1])) for col, i in index.items()}
        for o, index in bloom_index
        if index
    }

    dropped = []

    for object_id in object_ids:
        if object_id not in bloom_index:
            continue

        and_result = True
        for or_quals in quals:
            or_result = False
            for or_qual in or_quals:
                if _match(or_qual, bloom_index[object_id]):
                    or_result = True
                    break
            if not or_result:
                # One of the subclauses discarded this fragment.
                and_result = False
                break

        if not and_result:
            dropped.append(object_id)

    return [o for o in object_ids if o not in dropped]
