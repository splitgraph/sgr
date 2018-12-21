"""
Public API for calculating differences between two Splitgraph images.
"""

from psycopg2.sql import SQL, Identifier

from splitgraph._data.objects import get_object_for_table
from splitgraph.commands.misc import table_exists_at, find_path
from splitgraph.commands.tagging import get_current_head
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.engine import get_engine


def _changes_to_aggregation(query_result, initial=None):
    result = list(initial) if initial else [0, 0, 0]
    for kind, kind_count in query_result:
        result[kind] += kind_count
    return tuple(result)


def diff(repository, table_name, image_1, image_2, aggregate=False):
    """
    Compares the state of a table in different images. If the two images are on the same path in the commit tree,
    it doesn't need to materialize any of the tables and simply aggregates their DIFF objects to produce a complete
    changelog. Otherwise, it materializes both tables into a temporary space and compares them row-to-row.

    :param repository: Repository where the table exists.
    :param table_name: Name of the table.
    :param image_1: First image. If None, uses the state of the current staging area.
    :param image_2: Second image. If None, uses the state of the current staging area.
    :param aggregate: If True, returns a tuple of integers denoting added, removed and updated rows between
        the two images.
    :return: If the table doesn't exist in one of the images, returns True if it was added and False if it was removed.
        If `aggregate` is True, returns the aggregation of changes as specified before.
        Otherwise, returns a list of changes where each change is of the format (primary key, action_type, action_data):

            * `action_type == 0` is Insert and the `action_data` contains a dictionary of non-PK columns and values
              inserted.
            * `action_type == 1`: Delete, `action_data` is None.
            * `action_type == 2`: Update, `action_data` is a dictionary of non-PK columns and their new values for that
              particular row.
    """

    # If the table doesn't exist in the first or the second image, short-circuit and
    # return the bool.
    if not table_exists_at(repository, table_name, image_1):
        return True
    if not table_exists_at(repository, table_name, image_2):
        return False

    # Special case: if diffing HEAD and staging, then just return the current pending changes.
    head = get_current_head(repository)

    if image_1 == head and image_2 is None:
        changes = get_engine().get_pending_changes(repository.to_schema(), table_name, aggregate=aggregate)
        return list(changes) if not aggregate else _changes_to_aggregation(changes)

    # If the table is the same in the two images, short circuit as well.
    if image_2 is not None:
        if set(repository.get_image(image_1).get_table(table_name).objects) == \
                set(repository.get_image(image_2).get_table(table_name).objects):
            return [] if not aggregate else (0, 0, 0)

    # Otherwise, check if image_1 is a parent of image_2, then we can merge all the diffs.
    # FIXME: we have to find if there's a path between two _objects_ representing these tables that's made out of DIFFs.
    path = find_path(repository, image_1, (image_2 if image_2 is not None else head))
    if path is not None:
        result = _calculate_merged_diff(repository, table_name, path, aggregate)
        if result is None:
            return _side_by_side_diff(repository, table_name, image_1, image_2, aggregate)

        # If snap_2 is staging, also include all changes that have happened since the last commit.
        if image_2 is None:
            changes = get_engine().get_pending_changes(repository.to_schema(), table_name, aggregate=aggregate)
            if aggregate:
                return _changes_to_aggregation(changes, result)
            result.extend(changes)
        return result

    # Finally, resort to manual diffing (images on different branches or reverse comparison order).
    return _side_by_side_diff(repository, table_name, image_1, image_2, aggregate)


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


def has_pending_changes(repository):
    """
    Detects if the repository has any pending changes (schema changes, table additions/deletions, content changes).

    :param repository: Repository object
    """
    head = get_current_head(repository, raise_on_none=False)
    if not head:
        # If the repo isn't checked out, no point checking for changes.
        return False
    for table in get_engine().get_all_tables(repository.to_schema()):
        if diff(repository, table, head, None, aggregate=True) != (0, 0, 0):
            return True
    return False
