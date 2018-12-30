"""
sgr commands related to building and rebuilding Splitfiles.
"""

import click

from splitgraph.commandline._common import image_spec_parser
from splitgraph.core.repository import Repository
from splitgraph.splitfile import execute_commands, rebuild_image


@click.command(name='build')
@click.argument('splitfile', type=click.File('r'))
@click.option('-a', '--args', multiple=True, type=(str, str),
              help='Parameters to be substituted into the Splitfile. All parameters mentioned in the file'
                   ' must be specified in order for the Splitfile to be executed.')
@click.option('-o', '--output-repository', help='Repository to store the result in.',
              type=Repository.from_schema)
def build_c(splitfile, args, output_repository):
    """
    Build Splitgraph images.

    This executes a Splitfile, building a new image or checking it out from cache if the same
    image had already been built.

    Examples:

    ``sgr build my.splitfile``

        Executes ``my.splitfile`` and writes its output into a new repository with a random name unless
        the name is specified in the Splitfile.

    ``sgr build my.splitfile -o mynew/repo``

        Executes ``my.splitfile`` and writes its output into ``mynew/repo``.

    ``sgr build my_other.splitfile -o mynew/otherrepo --args PARAM1 VAL1 --args PARAM2 VAL2``

        Executes ``my_other.splitfile`` with parameters ``PARAM1`` and ``PARAM2`` set to
        ``VAL1`` and  ``VAL2``, respectively.
    """
    args = {k: v for k, v in args}
    print("Executing Splitfile %s with arguments %r" % (splitfile.name, args))
    execute_commands(splitfile.read(), args, output=output_repository)


@click.command(name='provenance')
@click.argument('image_spec', type=image_spec_parser())
@click.option('-f', '--full', required=False, is_flag=True, help='Recreate the Splitfile used to create this image')
@click.option('-e', '--error-on-end', required=False, default=True, is_flag=True,
              help='If False, bases the recreated Splitfile on the last image where the provenance chain breaks')
def provenance_c(image_spec, full, error_on_end):
    """
    Reconstruct the provenance of an image.

    This crawls the history of a Splitgraph image to produce a list of images that were used by the Splitfile
    that created it, or a Splitfile with the same effect.

    ``IMAGE_SPEC`` must be of the form ``[NAMESPACE/]REPOSITORY[:HASH_OR_TAG]``.
    If no tag is specified, ``latest`` is used.

    Examples:

    Assume ``my/repo`` is produced by the following Splitfile::

        FROM MOUNT [...] IMPORT external_table
        FROM noaa/climate IMPORT {SELECT * FROM rainfall_data WHERE state = 'AZ'} AS rainfall_data

    ``my/repo`` will have 2 images: one having ``hash_1`` (with the ``external_table`` imported from a mounted database)
    and one having ``hash_2`` (with both ``external_table`` and the ``rainfall_data`` containing the result
    of the query run against the then-latest image in the ``noaa/climate`` repository).

    In this case:

    ``sgr provenance my/repo``

        Returns a list of repositories and images that were imported by the Splitfile that constructed this image::

            my/repo:[hash_2] depends on:
            noaa/climate:[hash_3]

        Where ``hash_3`` is the hash of the latest image in the ``noaa/climate`` repository at the time the original
        Splitfile was run. However:

    ``sgr provenance -f my/repo``

        Will try to reconstruct the Splitfile that can be used to build this image. Since the FROM MOUNT command isn't
        reproducible (requires access to the original external database, which is a moving target), this will fail.

        If ``-e`` is passed, this will base the image on the first image that can't be reproduced::

            sgr provenance -ef my/repo

            # Splitfile commands used to reconstruct my/repo:[hash of the second layer]
            FROM my/repo:[hash_1]
            FROM noaa/climate:[hash_3] IMPORT {SELECT * FROM rainfall_data WHERE state = 'AZ'}
    """
    repository, image = image_spec
    image = repository.images[image]
    
    if full:
        splitfile_commands = image.to_splitfile(err_on_end=error_on_end)
        print("# Splitfile commands used to recreate %s:%s" % (str(repository), image))
        print('\n'.join(splitfile_commands))
    else:
        result = image.provenance()
        print("%s:%s depends on:" % (str(repository), image))
        print('\n'.join("%s:%s" % rs for rs in result))


@click.command(name='rebuild')
@click.argument('image_spec', type=image_spec_parser())
@click.option('-u', '--update', is_flag=True, help='Rederive the image against the latest version of all dependencies.')
@click.option('-a', '--against', multiple=True, type=image_spec_parser(),
              help='Images to substitute into the reconstructed Splitfile, of the form'
                   ' [NAMESPACE/]REPOSITORY[:HASH_OR_TAG]. Default tag is \'latest\'.')
def rebuild_c(image_spec, update, against):
    """
    Rebuild images against different dependencies.

    Examines the provenance of a Splitgraph image created by a Splitfile and reruns it against different images than
    the ones that were imported by the original run.

    Examples:

    ``sgr rebuild my/repo --against noaa/climate:old_data``

        Reconstructs the Splitfile used to create ``my/repo:latest``, replaces all imports from ``noaa/climate`` with
        imports from ``noaa/climate:old_data`` and reruns the Splitfile.

    ``sgr rebuild my/repo:other_tag -u``

        Rebuilds ``my_repo:other_tag`` against the latest versions of all of its dependencies.

        Image caching still works in this case: if the result of the rebuild already exists, the image will be checked
        out.
    """
    repository, image = image_spec
    image = repository.images[image]

    # Replace the sources used to construct the image with either the latest ones or the images specified by the user.
    # This doesn't require us at this point to have pulled all the dependencies: the Splitfile executor will do it
    # after we feed in the reconstructed and patched Splitfile.
    deps = {k: v for k, v in image.provenance()}
    new_images = {repo: repl_image for repo, repl_image in against} if not update \
        else {repo: 'latest' for repo, _ in deps.items()}
    deps.update(new_images)

    print("Rerunning %s:%s against:" % (str(repository), image.image_hash))
    print('\n'.join("%s:%s" % rs for rs in new_images.items()))

    rebuild_image(image, new_images)
