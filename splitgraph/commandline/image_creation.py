import sys
from collections import defaultdict

import click

import splitgraph as sg
from splitgraph.commandline.common import parse_image_spec
from splitgraph.pg_utils import get_all_foreign_tables

# TODO extra commands here: pruning (delete images that aren't pointed to by a tag) at the very least
# Also might be useful: squashing an image (turning all of its objects into SNAPs, creating a new image)


@click.command(name='checkout')
@click.argument('image_spec', type=parse_image_spec)
@click.option('-f', '--force', help="Discard all pending changes to the schema", is_flag=True, default=False)
def checkout_c(image_spec, force):
    """
    Checks out a Splitgraph image into a Postgres schema, discarding all pending changes, downloading the required
    objects and materializing all tables.

    Image spec must be of the format [NAMESPACE/]REPOSITORY[:HASH_OR_TAG]. Note that currently, the schema that the
    image is checked out into has to have the same name as the repository. If no image hash or tag is passed,
    "latest" is assumed.

    If --force isn't passed and the schema has pending changes, this will fail.
    """
    repository, image = image_spec
    image = sg.resolve_image(repository, image)
    sg.checkout(repository, image, force=force)
    print("Checked out %s:%s." % (str(repository), image[:12]))


@click.command(name='commit')
@click.argument('repository', type=sg.to_repository)
@click.option('-s', '--include-snap', default=False, is_flag=True,
              help='Include the full image snapshot. This consumes more space, '
                   'but makes checkouts faster.')
@click.option('-m', '--message', help='Optional commit message')
def commit_c(repository, include_snap, message):
    """
    Commits all changes to a checked-out Splitgraph repository, producing a new image.
    """
    new_hash = sg.commit(repository, include_snap=include_snap, comment=message)
    print("Committed %s as %s." % (str(repository), new_hash[:12]))


@click.command(name='tag')
@click.argument('repository', type=sg.to_repository)
@click.option('-i', '--image')
@click.argument('tag', required=False)
@click.option('-f', '--force', required=False, is_flag=True)
def tag_c(repository, image, tag, force):  # pylint disable=missing-docstring
    if tag is None:
        # List all tags
        tag_dict = defaultdict(list)
        for img, img_tag in sg.get_all_hashes_tags(repository):
            tag_dict[img].append(img_tag)
        if image is None:
            for img, tags in tag_dict.items():
                # Sometimes HEAD is none (if we've just cloned the repo)
                if img:
                    print("%s: %s" % (img[:12], ', '.join(tags)))
        else:
            print(', '.join(tag_dict[sg.get_canonical_image_id(repository, image)]))
        return

    if tag == 'HEAD':
        raise sg.SplitGraphException("HEAD is a reserved tag!")

    if image is None:
        image = sg.get_current_head(repository)
    else:
        image = sg.get_canonical_image_id(repository, image)
    sg.set_tag(repository, image, tag, force)
    print("Tagged %s:%s with %s." % (str(repository), image, tag))


@click.command(name='import')
@click.argument('image_spec', type=parse_image_spec)
@click.argument('table_or_query')
@click.argument('target_repository', type=sg.to_repository)
@click.argument('target_table', required=False)
def import_c(image_spec, table_or_query, target_repository, target_table):
    """
    Imports a table or a result of a query from a local Splitgraph repository or a Postgres schema into another
    Splitgraph repository.

    Examples:

        sgr import noaa/climate:my_tag climate_data my/repository

    Creates a new image in my/repository with the climate_data table included. Note this links the new image to the
    physical object, meaning that the history of the climate_data table is preserved.

        sgr import noaa/climate:my_tag "SELECT * FROM climate_data" my/repository climate_data

    Creates a new image in my/repository with the result of the query stored in the climate_data table. This
    creates a new physical object without any linkage to the original data, so the history of the climate_data
    table isn't preserved. The SQL query can interact with multiple tables in the source image.

        sgr import other_schema other_table my/repository

    Since other_schema isn't a Splitgraph repository, this will copy other_schema.other_table into a new Splitgraph
    object and add the other_table table to a new image in my/repository.

    Note that importing doesn't discard or commit pending changes in the target Splitgraph repository: a new image
    is created with the new table added, the new table is materialized in the repository and the HEAD pointer is moved.
    """
    repository, image = image_spec

    if sg.repository_exists(repository):
        foreign_table = False
        if not image:
            image = sg.get_current_head(repository)
        else:
            image = sg.resolve_image(repository, image)
        # If the source table doesn't exist in the image, we'll treat it as a query instead.
        is_query = not bool(sg.get_table(repository, table_or_query, image))
    else:
        # If the source schema isn't actually a Splitgraph repo, we'll be copying the table verbatim.
        foreign_table = True
        conn = sg.get_connection()
        is_query = table_or_query not in (sg.get_all_tables(conn, repository.to_schema())
                                          + get_all_foreign_tables(conn, repository.to_schema()))
        image = None

    if is_query and not target_table:
        print("TARGET_TABLE is required when the source is a query!")
        sys.exit(1)

    sg.import_tables(repository, [table_or_query], target_repository, [target_table] if target_table else [],
                     image_hash=image, foreign_tables=foreign_table, table_queries=[] if not is_query else [True])

    print("%s:%s has been imported from %s:%s%s" % (str(target_repository), target_table, str(repository),
                                                    table_or_query, (' (%s)' % image[:12] if image else '')))
