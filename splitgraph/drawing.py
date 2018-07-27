# coding=utf-8
from collections import defaultdict

from splitgraph.constants import SplitGraphException
from splitgraph.meta_handler import get_all_snap_parents, get_current_head


def calc_columns(children, start):
    def dfs(node):
        result = {node: 0}
        base = 0
        for child in children[node]:
            child_res = calc_columns(children, child)
            result.update({cname: ccol + base for cname, ccol in child_res.iteritems()})
            base += max(child_res.values()) + 1
        return result

    return dfs(start)


def render_node(node_id, children, node_cols, max_col, mark_node='', node_width=8, col_width=12):
    # First, render all the edges that come before the node
    line = ("│" + " " * (col_width - 1)) * node_cols[node_id]
    # Then, render the node itself.
    line += "├─" + node_id[:node_width] + mark_node + " " * (col_width - node_width - 2 - len(mark_node))
    # Finally, render all the edges on the right of the node.
    children_cols = set(node_cols[c] for c in children[node_id])

    for col in xrange(node_cols[node_id] + 1, max_col + 1):
        # If there's a child in this column, draw an inverse T.
        child_found = False
        for c in children[node_id]:
            if node_cols[c] == col:
                child_found = True
                # Draw a branch
                if col == max_col:
                    line += "┘"
                else:
                    line += "┴" + "─" * (col_width - 1)
                break
        if not child_found:
            # Otherwise, pad it with dashes.
            if not children_cols or col >= max(children_cols):
                line += '│' + " " * (col_width-1)
            else:
                line += "─" * col_width
    print line


def render_tree(conn, mountpoint):
    # Prepare the tree structure by loading the index from the db and flipping it
    children = defaultdict(list)
    base_node = None
    head = get_current_head(conn, mountpoint, raise_on_none=False)
    snap_parents = get_all_snap_parents(conn, mountpoint)
    for snap_id, parent_id in snap_parents:
        if parent_id is None:
            base_node = snap_id
        children[parent_id].append(snap_id)

    if base_node is None:
        raise SplitGraphException("Something is seriously wrong with the index.")

    # Calculate the column in which each node should be displayed.
    node_cols = calc_columns(children, base_node)

    # We don't have time information stored currently, but the current ordering in the snap_tree
    # table _should_ be chronological.
    for snap_id, _ in reversed(snap_parents):
        render_node(snap_id, children, node_cols, mark_node=' H' if snap_id == head else '', max_col=max(node_cols.values()))
