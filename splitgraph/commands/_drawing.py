# coding=utf-8
"""
Routines for rendering a Splitgraph repository as a tree of images
"""
from collections import defaultdict

from splitgraph._data.common import ensure_metadata_schema
from splitgraph._data.images import get_all_image_info
from splitgraph.exceptions import SplitGraphException


def _calc_columns(children, start):
    def dfs(node):
        result = {node: 0}
        base = 0
        for child in children[node]:
            child_res = _calc_columns(children, child)
            result.update({cname: ccol + base for cname, ccol in iter(child_res.items())})
            base += max(child_res.values()) + 1
        return result

    return dfs(start)


def _render_node(node_id, children, node_cols, max_col, mark_node='', node_width=8, col_width=12):
    # First, render all the edges that come before the node
    line = ("│" + " " * (col_width - 1)) * node_cols[node_id]
    # Then, render the node itself.
    line += "├─" + node_id[:node_width] + mark_node + " " * (col_width - node_width - 2 - len(mark_node))
    # Finally, render all the edges on the right of the node.
    children_cols = set(node_cols[c] for c in children[node_id])

    for col in range(node_cols[node_id] + 1, max_col + 1):
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
                line += '│' + " " * (col_width - 1)
            else:
                line += "─" * col_width
    print(line)


def render_tree(repository):
    """Draws the repository's commit graph as a Git-like tree."""
    ensure_metadata_schema()
    # Prepare the tree structure by loading the index from the db and flipping it
    children = defaultdict(list)
    base_node = None
    head = repository.get_head()

    # Get all commits in ascending time order
    snap_parents = get_all_image_info(repository)
    for image_hash, parent_id, _, _, _, _ in snap_parents:
        if parent_id is None:
            base_node = image_hash
        children[parent_id].append(image_hash)

    if base_node is None:
        raise SplitGraphException("Something is seriously wrong with the index.")

    # Calculate the column in which each node should be displayed.
    node_cols = _calc_columns(children, base_node)

    for image_hash, _, _, _, _, _ in reversed(snap_parents):
        _render_node(image_hash, children, node_cols, mark_node=' H' if image_hash == head else '',
                     max_col=max(node_cols.values()))
