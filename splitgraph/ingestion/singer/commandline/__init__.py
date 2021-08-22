"""Command line tools for building Splitgraph images from Singer taps, including using Splitgraph as a Singer target."""
import click

from splitgraph.commandline.common import ImageType
from splitgraph.hooks.data_source.base import prepare_new_image


@click.group(name="singer")
def singer_group():
    """Build Splitgraph images from Singer taps."""


@click.command(name="target")
@click.argument("image", type=ImageType(default="latest", repository_exists=False))
@click.option(
    "-d", "--delete-old", is_flag=True, help="Delete the old image at the end of ingestion"
)
@click.option(
    "-f",
    "--failure",
    type=click.Choice(["keep-both", "delete-old", "delete-new"]),
    help="What to do in case of a failure.",
    default="delete-new",
)
def singer_target(image, delete_old, failure):
    """
    Singer target that loads data into Splitgraph images.

    This will read data from the stdin from a Singer-compatible tap and load it into
    a Splitgraph image, merging data if the image already exists.

    Image must be of the format `[NAMESPACE/]REPOSITORY[:HASH_OR_TAG]` where `HASH_OR_TAG`
    is a tag of an existing image to base the image on. If the repository doesn't exist,
    it will be created.

    As this target consumes data from stdin, it will flush the records into a Splitgraph image. By
    default, it will only keep the image if the whole stream has been successfully consumed. To
    make this target completely follow the Singer spec (if it emits state, the records have been
    flushed), pass --failure=keep-both or --failure=delete-old. To delete the old image on success,
    pass --delete-old.
    """
    from splitgraph.ingestion.singer.db_sync import run_patched_sync

    repository, hash_or_tag = image
    base_image, new_image_hash = prepare_new_image(repository, hash_or_tag)
    run_patched_sync(repository, base_image, new_image_hash, delete_old, failure)


singer_group.add_command(singer_target)
