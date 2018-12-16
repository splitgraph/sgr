"""
Internal helper functions for packaging changes recorded by the audit trigger into Splitgraph objects.
"""

from random import getrandbits

from splitgraph.exceptions import SplitGraphException


def _merge_changes(old_change_data, new_change_data):
    old_change_data = {k: v for k, v in zip(old_change_data['c'], old_change_data['v'])}
    old_change_data.update({k: v for k, v in zip(new_change_data['c'], new_change_data['v'])})
    return {'c': list(old_change_data.keys()), 'v': list(old_change_data.values())}


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
