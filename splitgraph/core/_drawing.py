# coding=utf-8
"""
Routines for rendering a Splitgraph repository as a tree of images
"""
from collections import defaultdict

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
        for child in children[node_id]:
            if node_cols[child] == col:
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
    # Prepare the tree structure by loading the index from the db and flipping it
    children = defaultdict(list)
    base_node = None
    head = repository.head.image_hash if repository.head else None

    # Get all commits in ascending time order
    all_images = repository.images()
    for image in all_images:
        if image.parent_id is None:
            base_node = image.image_hash
        children[image.parent_id].append(image.image_hash)

    if base_node is None:
        raise SplitGraphException("Something is seriously wrong with the index.")

    # Calculate the column in which each node should be displayed.
    node_cols = _calc_columns(children, base_node)

    for image in reversed(all_images):
        _render_node(image.image_hash, children, node_cols, mark_node=' H' if image.image_hash == head else '',
                     max_col=max(node_cols.values()))
