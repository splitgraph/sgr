import click

import splitgraph as sg


@click.command(name='build')
@click.argument('splitfile', type=click.File('r'))
@click.option('-a', '--args', multiple=True, type=(str, str))
@click.option('-o', '--output-repository', help='Repository to store the result in.',
              type=sg.to_repository)
def build_c(splitfile, args, output_repository):  # pylint disable=missing-docstring
    args = {k: v for k, v in args}
    print("Executing Splitfile %s with arguments %r" % (splitfile.name, args))
    sg.execute_commands(splitfile.read(), args, output=output_repository)


@click.command(name='provenance')
@click.argument('repository', type=sg.to_repository)
@click.argument('snapshot_or_tag')
@click.option('-f', '--full', required=False, is_flag=True, help='Recreate the Splitfile used to create this image')
@click.option('-e', '--error-on-end', required=False, default=True, is_flag=True,
              help='If False, bases the recreated Splitfile on the last image where the provenance chain breaks')
def provenance_c(repository, snapshot_or_tag, full, error_on_end):  # pylint disable=missing-docstring
    snapshot = sg.resolve_image(repository, snapshot_or_tag)

    if full:
        splitfile_commands = sg.image_hash_to_splitfile(repository, snapshot, error_on_end)
        print("# Splitfile commands used to recreate %s:%s" % (str(repository), snapshot))
        print('\n'.join(splitfile_commands))
    else:
        result = sg.provenance(repository, snapshot)
        print("%s:%s depends on:" % (str(repository), snapshot))
        print('\n'.join("%s:%s" % rs for rs in result))


@click.command(name='rebuild')
@click.argument('repository', type=sg.to_repository)
@click.argument('snapshot_or_tag')
@click.option('-u', '--update', is_flag=True, help='Rederive the image against the latest version of all dependencies.')
@click.option('-i', '--repo-image', multiple=True, type=(sg.to_repository, str))
def rebuild_c(repository, snapshot_or_tag, update, repo_image):  # pylint disable=missing-docstring
    snapshot = sg.resolve_image(repository, snapshot_or_tag)

    # Replace the sources used to construct the image with either the latest ones or the images specified by the user.
    # This doesn't require us at this point to have pulled all the dependencies: the Splitfile executor will do it
    # after we feed in the reconstructed and patched Splitfile.
    deps = {k: v for k, v in sg.provenance(repository, snapshot)}
    new_images = {repo: image for repo, image in repo_image} if not update \
        else {repo: 'latest' for repo, _ in deps.items()}
    deps.update(new_images)

    print("Rerunning %s:%s against:" % (str(repository), snapshot))
    print('\n'.join("%s:%s" % rs for rs in new_images.items()))

    sg.rerun_image_with_replacement(repository, snapshot, new_images)
