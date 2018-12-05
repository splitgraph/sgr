"""
Internal helper functions for packaging changes recorded by the audit trigger into Splitgraph objects.
"""

from random import getrandbits

from splitgraph.connection import get_connection
from splitgraph.exceptions import SplitGraphException
from splitgraph.pg_utils import get_primary_keys, get_column_names_types


def get_replica_identity(schema, table):
    """
    Get the PK we're storing changes as. If the table has no PK, we treat the whole row as the primary key,
    meaning that we don't support
    """
    conn = get_connection()
    return get_primary_keys(conn, schema, table) or get_column_names_types(conn, schema, table)


def _split_ri_cols(action, row_data, changed_fields, ri_cols):
    """
    :return: `(ri_vals, non_ri_cols, non_ri_vals)`: a tuple of 3 lists:
        * `ri_vals`: values identifying the replica identity (RI) of a given tuple (matching column names in `ri_cols`)
        * `non_ri_cols`: column names not in the RI that have been changed/updated
        * `non_ri_vals`: column values not in the RI that have been changed/updated (matching colnames in `non_ri_cols`)
    """
    non_ri_cols = []
    non_ri_vals = []
    ri_vals = [None] * len(ri_cols)

    if action == 'I':
        for cc, cv in row_data.items():
            if cc in ri_cols:
                ri_vals[ri_cols.index(cc)] = cv
            else:
                non_ri_cols.append(cc)
                non_ri_vals.append(cv)
    elif action == 'D':
        for cc, cv in row_data.items():
            if cc in ri_cols:
                ri_vals[ri_cols.index(cc)] = cv
    elif action == 'U':
        for cc, cv in row_data.items():
            if cc in ri_cols:
                ri_vals[ri_cols.index(cc)] = cv
        for cc, cv in changed_fields.items():
            # Hmm: these might intersect with the RI values (e.g. when the whole tuple is the replica identity and
            # we're updating some of it)
            non_ri_cols.append(cc)
            non_ri_vals.append(cv)

    return ri_vals, non_ri_cols, non_ri_vals


def _recalculate_disjoint_ri_cols(ri_cols, ri_vals, non_ri_cols, non_ri_vals):
    # Move the intersecting columns from the non-ri to the ri set
    new_nric = []
    new_nriv = []
    for nrc, nrv in zip(non_ri_cols, non_ri_vals):
        try:
            ri_vals[ri_cols.index(nrc)] = nrv
        except ValueError:
            new_nric.append(nrc)
            new_nriv.append(nrv)
    return ri_vals, new_nric, new_nriv


def _merge_changes(old_change_data, new_change_data):
    old_change_data = {k: v for k, v in zip(old_change_data['c'], old_change_data['v'])}
    old_change_data.update({k: v for k, v in zip(new_change_data['c'], new_change_data['v'])})
    return {'c': list(old_change_data.keys()), 'v': list(old_change_data.values())}


def convert_audit_change(action, row_data, changed_fields, ri_cols):
    """
    Converts the audit log entry into Splitgraph's internal format.
    :returns: [(pk, kind, extra data)] (more than 1 change might be emitted from a single audit entry).
    """
    ri_vals, non_ri_cols, non_ri_vals = _split_ri_cols(action, row_data, changed_fields, ri_cols)
    pk_changed = any(c in ri_cols for c in non_ri_cols)
    if pk_changed:
        assert action == 'U'
        # If it's an update that changed the PK (e.g. the table has no replica identity so we treat the whole
        # tuple as a primary key), then we turn it into a delete old tuple + insert new one.
        # This might happen with updates in any case, since the WAL seems to output the old and the new values for the
        # PK no matter what the replica identity settings are. However, the resulting delete + insert
        # gets conflated back into an update in any case if the PK is the same between the two.
        result = [(tuple(ri_vals), 1, None)]
        # todo: will this work if a part of the primary key + some other column in the tuple has been updated?
        ri_vals, non_ri_cols, non_ri_vals = _recalculate_disjoint_ri_cols(ri_cols, ri_vals, non_ri_cols, non_ri_vals)
        result.append((tuple(ri_vals), 0, {'c': non_ri_cols, 'v': non_ri_vals}))
        return result
    return [(tuple(ri_vals), KIND[action],
             {'c': non_ri_cols, 'v': non_ri_vals} if action in ('I', 'U') else None)]


def conflate_changes(changeset, new_changes):
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
                    raise SplitGraphException("TODO logic error")
            elif change_kind == 1:  # Delete over insert/update: remove the old change
                del changeset[change_pk]
                if old_change[0] == 2:
                    # If it was an update, also remove the old row.
                    changeset[change_pk] = (1, change_data)
                if old_change[0] == 1:
                    # Delete over delete: can't happen.
                    raise SplitGraphException("TODO logic error")
            elif change_kind == 2:  # Update over insert/update: merge the two changes.
                if old_change[0] == 0 or old_change[0] == 2:
                    new_data = _merge_changes(old_change[1], change_data)
                    changeset[change_pk] = (old_change[0], new_data)


# "Truncate" kind is missing
KIND = {'I': 0, 'D': 1, 'U': 2}


def get_random_object_id():
    """Assign each table a random ID that it will be stored as. Note that postgres limits table names to 63 characters,
    so the IDs shall be 248-bit strings, hex-encoded, + a letter prefix since Postgres doesn't seem to support table
    names starting with a digit."""
    # Make sure we're padded to 62 characters (otherwise if the random number generated is less than 2^247 we'll be
    # dropping characters from the hex format)
    return str.format('o{:062x}', getrandbits(248))
