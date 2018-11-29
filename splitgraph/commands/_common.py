"""
Common internal functions used by Splitgraph commands.
"""

from splitgraph.commands.tagging import set_tag


def set_head(repository, image):
    """Sets the HEAD pointer of a given repository to a given image. Shouldn't be used directly."""
    set_tag(repository, image, 'HEAD', force=True)
