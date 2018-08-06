from splitgraph.commands.checkout import materialized_table
from splitgraph.commands.misc import _table_exists_at, _find_path
from splitgraph.meta_handler import get_current_head, get_table, get_table_with_format
from splitgraph.pg_replication import dump_pending_changes


def diff(conn, mountpoint, table_name, snap_1, snap_2):
    # Returns a list of changes done to a table if it exists in both images.
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
        # TODO do we need to conflate them e.g. if A changed to B and then B changed to C, do we emit A -> C?
        if snap_1 == HEAD and snap_2 is None:
            return dump_pending_changes(conn, mountpoint, table_name)

        # If the table is the same in the two images, short circuit as well.
        if set(get_table(conn, mountpoint, table_name, snap_1)) == set(get_table(conn, mountpoint, table_name, snap_2)):
            return []

        # Otherwise, check if snap_1 is a parent of snap_2, then we can merge all the diffs.
        path = _find_path(conn, mountpoint, snap_1, (snap_2 if snap_2 is not None else HEAD))
        if path is not None:
            result = []
            for image in reversed(path):
                diff_id = get_table_with_format(conn, mountpoint, table_name, image, 'DIFF')
                cur.execute("""SELECT kind, change FROM %s""" % cur.mogrify('%s.%s' % (mountpoint, diff_id)))
                result.extend(cur.fetchall())

            # If snap_2 is staging, also include all changes that have happened since the last commit.
            if snap_2 is None:
                result.extend(dump_pending_changes(conn, mountpoint, table_name))
            return result

        else:
            # Finally, resort to manual diffing (images on different branches or reverse comparison order).
            with materialized_table(conn, mountpoint, table_name, snap_1) as table_1:
                with materialized_table(conn, mountpoint, table_name, snap_2) as table_2:
                    # Check both tables out at the same time since then table_2 calculation can be based
                    # on table_1's snapshot.
                    cur.execute("""SELECT * FROM %s""" % cur.mogrify('%s.%s' % (mountpoint, table_1)))
                    left = cur.fetchall()
                    cur.execute("""SELECT * FROM %s""" % cur.mogrify('%s.%s' % (mountpoint, table_2)))
                    right = cur.fetchall()

            # Mimic the diff format returned by the WAL
            return [(1, r) for r in left if r not in right] + [(0, r) for r in right if r not in left]