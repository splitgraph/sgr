import click

import splitgraph as sg
from splitgraph._data.images import _get_all_child_images, delete_images
from splitgraph.commandline.common import image_spec_parser


@click.command(name='rm')
@click.argument('image_spec', type=image_spec_parser(default=None))
@click.option('-r', '--remote', help="Perform the deletion on a remote instead, specified by its alias")
@click.option('-y', '--yes', help="Agree to deletion without confirmation", is_flag=True, default=False)
def rm_c(image_spec, remote, yes):
    """
    Delete schemas, repositories or images.

    If the target of this command is a Postgres schema, this performs DROP SCHEMA CASCADE.

    If the target of this command is a Splitgraph repository, this deletes the repository and all of its history.

    If the target of this command is an image, this deletes the image and all of its children.

    In any case, this command will ask for confirmation of the deletion, unless -y is passed. If -r (--remote), is
    passed, this will perform deletion on a remote Splitgraph driver (registered in the config) instead, assuming
    the user has write access to the remote repository.

    This does not delete any physical objects that the deleted repository/images depend on:
    use `sgr cleanup` to do that.

    Examples:

        sgr rm temporary_schema

    Deletes `temporary_schema` from the local driver.

        sgr rm --driver splitgraph.com username/repo

    Deletes `username/repo` from the Splitgraph registry.

        sgr rm -y username/repo:old_branch

    Deletes the image pointed to by `old_branch` as well as all of its children (images created by a commit based
    on this image), as well as all of the tags that point to now deleted images, without asking for confirmation. Note
    this will not delete images that import tables from the deleted images via Splitfiles or indeed the physical
    objects containing the actual tables.
    """

    repository, image = image_spec
    with sg.do_in_driver(remote):
        if not image:
            print(("Repository" if sg.repository_exists(repository) else "Postgres schema")
                  + " %s will be deleted." % repository.to_schema())
            if not yes:
                click.confirm("Continue? ", abort=True)
            sg.rm(repository)
        else:
            image = sg.resolve_image(repository, image)
            images_to_delete = _get_all_child_images(repository, image)

            # TODO Some issues here with the HEAD: if we're deleting an image that we currently have checked out,
            # we need to make sure the rest of the metadata (e.g. current state of the audit table) is consistent.
            # Here's an idea for handling this: we don't allow deletions where the current HEAD will get deleted.
            # We will, however, allow a "soft reset": checkout an image _without suspending the audit trigger_
            # (so the actual application of objects will be recorded in the audit again) and without moving the
            # HEAD pointer.
            # Maybe we'll also need an "uncheckout" command that deletes the current staging are, cleans up the audit
            # and removes the HEAD pointer (bringing the repo to its bare state).

            tags_to_delete = [t for i, t in sg.get_all_hashes_tags(repository) if i in images_to_delete and t != 'HEAD']

            print("Images to be deleted:")
            print("\n".join(sorted(images_to_delete)))
            print("Total: %d" % len(images_to_delete))

            print("\nTags to be deleted:")
            print("\n".join(sorted(tags_to_delete)))
            print("Total: %d" % len(tags_to_delete))

            if not yes:
                click.confirm("Continue? ", abort=True)

            delete_images(repository, images_to_delete)
            print("Success.")


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
