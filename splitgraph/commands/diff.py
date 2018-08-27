import json

from psycopg2.sql import SQL, Identifier

from splitgraph.commands.checkout import materialized_table
from splitgraph.commands.misc import table_exists_at, find_path
from splitgraph.constants import SPLITGRAPH_META_SCHEMA
from splitgraph.meta_handler import get_current_head, get_table, get_table_with_format
from splitgraph.pg_replication import dump_pending_changes


def _changes_to_aggregation(query_result, initial=None):
    result = list(initial) if initial else [0, 0, 0]
    for kind, kind_count in query_result:
        result[kind] += kind_count
    return tuple(result)


def diff(conn, mountpoint, table_name, image_1, image_2, aggregate=False):
    """
    Compares the state of the table in different images. If the two images are on the same path in the commit tree,
    it doesn't need to materialize any of the tables and simply aggregates their DIFF objects to produce a complete
    changelog. Otherwise, it materializes both tables into a temporary space and compares them row-to-row.
    :param conn: psycopg connection
    :param mountpoint: Mounpoint where the table exists.
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

    with conn.cursor() as cur:
        # If the table doesn't exist in the first or the second image, short-circuit and
        # return the bool.
        if not table_exists_at(conn, mountpoint, table_name, image_1):
            return True
        if not table_exists_at(conn, mountpoint, table_name, image_2):
            return False

        # Special case: if diffing HEAD and staging, then just return the current pending changes.
        head = get_current_head(conn, mountpoint)

        if image_1 == head and image_2 is None:
            changes = dump_pending_changes(conn, mountpoint, table_name, aggregate=aggregate)
            return changes if not aggregate else _changes_to_aggregation(changes)

        # If the table is the same in the two images, short circuit as well.
        if set(get_table(conn, mountpoint, table_name, image_1)) == \
                set(get_table(conn, mountpoint, table_name, image_2)):
            return [] if not aggregate else (0, 0, 0)

        # Otherwise, check if snap_1 is a parent of snap_2, then we can merge all the diffs.
        path = find_path(conn, mountpoint, image_1, (image_2 if image_2 is not None else head))
        if path is not None:
            result = [] if not aggregate else (0, 0, 0)
            for image in reversed(path):
                diff_id = get_table_with_format(conn, mountpoint, table_name, image, 'DIFF')
                if diff_id is None:
                    # TODO This entry on the path between the two nodes is a snapshot -- meaning there
                    # has been a schema change and we can't just accumulate diffs. For now, we just pretend
                    # it didn't happen and dump the data changes.
                    continue
                if not aggregate:
                    cur.execute(SQL("""SELECT * FROM {}.{}""").format(
                        Identifier(SPLITGRAPH_META_SCHEMA), Identifier(diff_id)))
                    # There's only one action applied to a tuple in a single diff, so the ordering doesn't matter.
                    image_diff = sorted(cur.fetchall())
                    for row in image_diff:
                        pk = row[:-2]
                        action = row[-2]
                        action_data = json.loads(row[-1]) if row[-1] else None
                        result.append((pk, action, action_data))
                else:
                    cur.execute(SQL(
                        """SELECT sg_action_kind, count(sg_action_kind) FROM {}.{} GROUP BY sg_action_kind""").format(
                        Identifier(SPLITGRAPH_META_SCHEMA), Identifier(diff_id)))
                    result = _changes_to_aggregation(cur.fetchall(), result)

            # If snap_2 is staging, also include all changes that have happened since the last commit.
            if image_2 is None:
                changes = dump_pending_changes(conn, mountpoint, table_name, aggregate=aggregate)
                if aggregate:
                    result = _changes_to_aggregation(changes, result)
                else:
                    result.extend(changes)
            return result

        # Finally, resort to manual diffing (images on different branches or reverse comparison order).
        with materialized_table(conn, mountpoint, table_name, image_1) as table_1:
            with materialized_table(conn, mountpoint, table_name, image_2) as table_2:
                # Check both tables out at the same time since then table_2 calculation can be based
                # on table_1's snapshot.
                cur.execute(SQL("SELECT * FROM {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                              Identifier(table_1)))
                left = cur.fetchall()
                cur.execute(SQL("SELECT * FROM {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                              Identifier(table_2)))
                right = cur.fetchall()

        # Mimic the diff format returned by the WAL
        if aggregate:
            return sum(1 for r in right if r not in left), sum(1 for r in left if r not in right), 0
        return [(1, r) for r in left if r not in right] + [(0, r) for r in right if r not in left]
