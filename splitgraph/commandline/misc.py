import click

import splitgraph as sg


@click.command(name='rm')
@click.argument('repository', type=sg.to_repository)
def rm_c(repository):
    """
    Delete a Postgres schema from the driver.

    If the schema represents a repository, this also deletes the repository and all of its history.

    This does not delete any physical objects that the repository depends on: use `sgr cleanup` to do that.
    """
    sg.rm(repository)


@click.command(name='init')
@click.argument('repository', type=sg.to_repository)
def init_c(repository):
    """
    Create an empty Splitgraph repository.

    This creates a single image with the hash 00000... in the new repository.
    """
    sg.init(repository)
    print("Initialized empty repository %s" % str(repository))


@click.command(name='cleanup')
def cleanup_c():
    """
    Prune unneeded objects from the driver.

    This deletes all objects from the cache that aren't required by any local repository.
    """
    deleted = sg.cleanup_objects()
    print("Deleted %d physical object(s)" % len(deleted))
