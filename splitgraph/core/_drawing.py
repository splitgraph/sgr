"""
Routines for rendering a Splitgraph repository as a tree of images
"""
from collections import defaultdict, OrderedDict
from datetime import datetime
from typing import TYPE_CHECKING, List

import click

from splitgraph.commandline.common import Color
from splitgraph.config import SG_CMD_ASCII

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository


def _pull_up_children(tree):
    """Pull up nodes with only one child to avoid ladders for long
    chains of commits on one branch. Technically, asciitree draws vertical trees,
    but we try to flatten them a bit."""

    # TODO this creates ambiguous trees where a line of commits renders the same
    #   as a bunch of siblings of a single node.
    new_tree = OrderedDict()
    one_child = len(tree) == 1

    for child_name, child in tree.items():
        flatten = len(child) == 1
        child = _pull_up_children(child)

        # If this node has a single child that itself only had a single child before
        # flattening, we can pull that tree's new children up (straighten the branch).
        if flatten and one_child:
            new_tree[child_name] = OrderedDict()
            new_tree.update(child)
        else:
            new_tree[child_name] = child

    return new_tree


def format_image_hash(image_hash: str) -> str:
    return Color.BOLD + Color.RED + image_hash[:10] + Color.END


def format_tags(tags: List[str]) -> str:
    if tags:
        return Color.BOLD + Color.YELLOW + " [" + ", ".join(tags) + "]" + Color.END
    return ""


def format_time(time: datetime) -> str:
    return " " + Color.GREEN + time.strftime("%Y-%m-%d %H:%M:%S") + Color.END


def render_tree(repository: "Repository") -> None:
    """Draws the repository's commit graph as a Git-like tree."""

    import asciitree
    from splitgraph.core.output import truncate_line

    # Get all commits in ascending time order
    all_images = {i.image_hash: i for i in repository.images()}

    if not all_images:
        return

    latest = repository.images["latest"]

    tag_dict = defaultdict(list)
    for img, img_tag in repository.get_all_hashes_tags():
        tag_dict[img].append(img_tag)
    tag_dict[latest.image_hash].append("latest")

    class ImageTraversal(asciitree.DictTraversal):
        def get_text(self, node):
            image = all_images[node[0]]
            result = format_image_hash(image.image_hash)
            result += format_tags(tag_dict[image.image_hash])
            result += format_time(image.created)
            if image.comment:
                result += " " + truncate_line(image.comment)
            return result

    tree: OrderedDict[str, OrderedDict] = OrderedDict(
        (image, OrderedDict()) for image in all_images
    )
    tree_elements = tree.copy()

    # Join children to parents to prepare a tree structure for asciitree
    for image in all_images.values():
        if image.parent_id is None or image.parent_id not in tree_elements:
            # If we only pulled a single image, it's possible that we won't have
            # the metadata for the image's parent.
            continue

        tree_elements[image.parent_id][image.image_hash] = tree_elements[image.image_hash]
        del tree[image.image_hash]

    # tree = _pull_up_children(tree)

    renderer = asciitree.LeftAligned(
        draw=asciitree.BoxStyle(
            gfx=asciitree.drawing.BOX_ASCII if SG_CMD_ASCII else asciitree.drawing.BOX_LIGHT,
            label_space=1,
            horiz_len=0,
        ),
        traverse=ImageTraversal(),
    )
    for root, root_tree in tree.items():
        click.echo(renderer({root: root_tree}))
