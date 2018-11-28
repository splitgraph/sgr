from splitgraph.commands.tagging import set_tag


def set_head(repository, image):
    set_tag(repository, image, 'HEAD', force=True)
