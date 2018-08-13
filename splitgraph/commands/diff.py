from psycopg2.sql import SQL, Identifier
import json

from splitgraph.commands.checkout import materialized_table
from splitgraph.commands.misc import _table_exists_at, _find_path
from splitgraph.meta_handler import get_current_head, get_table, get_table_with_format
from splitgraph.pg_replication import dump_pending_changes, record_pending_changes


def _changes_to_aggregation(query_result, initial=None):
    result = list(initial) if initial else [0, 0, 0]
    for kind, kind_count in query_result:
        result[kind] += kind_count
    return tuple(result)


def diff(conn, mountpoint, table_name, snap_1, snap_2, aggregate=False):
    record_pending_changes(conn)
    # Returns a list of changes done to a table if it exists in both images (if aggregate=False).
    # If aggregate=True, returns a triple showing the number of (added, removed, updated) rows.
    # Otherwise, returns True if the table was added and False if it was removed.
    with conn.cursor() as cur:
        # If the table doesn't exist in the first or the second image, short-circuit and
        # return the bool.
        if not _table_exists_at(conn, mountpoint, table_name, snap_1):
            return True
        if not _table_exists_at(conn, mountpoint, table_name, snap_2):
            return False

        # Special case: if diffing HEAD and staging, then just return the current pending changes.
        HEAD = get_current_head(conn, mountpoint)

        if snap_1 == HEAD and snap_2 is None:
            changes = dump_pending_changes(conn, mountpoint, table_name, aggregate=aggregate)
            return changes if not aggregate else _changes_to_aggregation(changes)

        # If the table is the same in the two images, short circuit as well.
        if set(get_table(conn, mountpoint, table_name, snap_1)) == set(get_table(conn, mountpoint, table_name, snap_2)):
            return [] if not aggregate else (0, 0, 0)

        # Otherwise, check if snap_1 is a parent of snap_2, then we can merge all the diffs.
        path = _find_path(conn, mountpoint, snap_1, (snap_2 if snap_2 is not None else HEAD))
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
                        Identifier(mountpoint), Identifier(diff_id)))
                    for row in cur:
                        pk = row[:-2]
                        action = row[-2]
                        action_data = json.loads(row[-1]) if row[-1] else None
                        result.append((pk, action, action_data))
                else:
                    cur.execute(SQL("""SELECT sg_action_kind, count(sg_action_kind) FROM {}.{} GROUP BY sg_action_kind""").format(
                        Identifier(mountpoint), Identifier(diff_id)))
                    result = _changes_to_aggregation(cur.fetchall(), result)

            # If snap_2 is staging, also include all changes that have happened since the last commit.
            if snap_2 is None:
                changes = dump_pending_changes(conn, mountpoint, table_name, aggregate=aggregate)
                if aggregate:
                    result = _changes_to_aggregation(changes, result)
                else:
                    result.extend(changes)
            return result

        else:
            # Finally, resort to manual diffing (images on different branches or reverse comparison order).
            with materialized_table(conn, mountpoint, table_name, snap_1) as table_1:
                with materialized_table(conn, mountpoint, table_name, snap_2) as table_2:
                    # Check both tables out at the same time since then table_2 calculation can be based
                    # on table_1's snapshot.
                    cur.execute(SQL("SELECT * FROM {}.{}").format(Identifier(mountpoint), Identifier(table_1)))
                    left = cur.fetchall()
                    cur.execute(SQL("SELECT * FROM {}.{}").format(Identifier(mountpoint), Identifier(table_2)))
                    right = cur.fetchall()

            # Mimic the diff format returned by the WAL
            if aggregate:
                return sum(1 for r in right if r not in left), sum(1 for r in left if r not in right), 0
            else:
                return [(1, r) for r in left if r not in right] + [(0, r) for r in right if r not in left]