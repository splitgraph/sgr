import click

import splitgraph as sg
from splitgraph.commandline.common import image_spec


@click.command(name='build')
@click.argument('splitfile', type=click.File('r'))
@click.option('-a', '--args', multiple=True, type=(str, str),
              help='Parameters to be substituted into the Splitfile. All parameters mentioned in the file'
                   ' must be specified in order for the Splitfile to be executed.')
@click.option('-o', '--output-repository', help='Repository to store the result in.',
              type=sg.to_repository)
def build_c(splitfile, args, output_repository):
    """
    Executes a Splitfile, building a Splitgraph image or checking it out from cache if the same
    image has already been built.
    """
    args = {k: v for k, v in args}
    print("Executing Splitfile %s with arguments %r" % (splitfile.name, args))
    sg.execute_commands(splitfile.read(), args, output=output_repository)


@click.command(name='provenance')
@click.argument('image_spec', type=image_spec())
@click.option('-f', '--full', required=False, is_flag=True, help='Recreate the Splitfile used to create this image')
@click.option('-e', '--error-on-end', required=False, default=True, is_flag=True,
              help='If False, bases the recreated Splitfile on the last image where the provenance chain breaks')
def provenance_c(image_spec, full, error_on_end):
    """
    Crawls the history of a Splitgraph image to reconstruct its provenance (a list of images that
    were used by the Splitfile that created it).

    IMAGE_SPEC must be of the form [NAMESPACE/]REPOSITORY[:HASH_OR_TAG]. If no tag is specified, 'latest' is used.
    """
    repository, image = image_spec
    image = sg.resolve_image(repository, image)
    
    if full:
        splitfile_commands = sg.image_hash_to_splitfile(repository, image, error_on_end)
        print("# Splitfile commands used to recreate %s:%s" % (str(repository), image))
        print('\n'.join(splitfile_commands))
    else:
        result = sg.provenance(repository, image)
        print("%s:%s depends on:" % (str(repository), image))
        print('\n'.join("%s:%s" % rs for rs in result))


@click.command(name='rebuild')
@click.argument('image_spec', type=image_spec())
@click.option('-u', '--update', is_flag=True, help='Rederive the image against the latest version of all dependencies.')
@click.option('-a', '--against', multiple=True, type=image_spec(),
              help='Images to substitute into the reconstructed Splitfile, of the form'
                   ' [NAMESPACE/]REPOSITORY[:HASH_OR_TAG]. Default tag is \'latest\'.')
def rebuild_c(image_spec, update, against):
    """
    Examines the provenance of a Splitgraph image created by a Splitfile and reruns it against different images than
    the ones that were imported by the original run.

    Examples

        sgr rebuild my_repo --against noaa/climate:old_data

    Reconstructs the Splitfile used to create `my_repo:latest`, replaces all imports from `noaa/climate` with
    imports from `noaa/climate:old_data` and reruns the Splitfile.

        sgr rebuild my_repo:other_tag -u

    Rebuilds my_repo:other_tag against the latest versions of all of its dependencies.

    Image caching still works in this case: if the result of the rebuild already exists, the image will be checked out.
    """
    repository, image = image_spec
    image = sg.resolve_image(repository, image)

    # Replace the sources used to construct the image with either the latest ones or the images specified by the user.
    # This doesn't require us at this point to have pulled all the dependencies: the Splitfile executor will do it
    # after we feed in the reconstructed and patched Splitfile.
    deps = {k: v for k, v in sg.provenance(repository, image)}
    new_images = {repo: image for repo, image in against} if not update \
        else {repo: 'latest' for repo, _ in deps.items()}
    deps.update(new_images)

    print("Rerunning %s:%s against:" % (str(repository), image))
    print('\n'.join("%s:%s" % rs for rs in new_images.items()))

    sg.rerun_image_with_replacement(repository, image, new_images)
