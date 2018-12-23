"""
sgr commands related to creating and checking out images
"""

import sys
from collections import defaultdict

import click

import splitgraph.core.repository
from splitgraph import SplitGraphException
from splitgraph.commandline._common import image_spec_parser
from splitgraph.core.engine import repository_exists


@click.command(name='checkout')
@click.argument('image_spec', type=image_spec_parser(default='HEAD'))
@click.option('-f', '--force', help="Discard all pending changes to the schema", is_flag=True, default=False)
@click.option('-u', '--uncheckout', help="Delete the checked out copy instead", is_flag=True, default=False)
def checkout_c(image_spec, force, uncheckout):
    """
    Check out a Splitgraph image into a Postgres schema.

    This downloads the required physical objects and materializes all tables.

    Image spec must be of the format ``[NAMESPACE/]REPOSITORY[:HASH_OR_TAG]``. Note that currently, the schema that the
    image is checked out into has to have the same name as the repository. If no image hash or tag is passed,
    "HEAD" is assumed.

    If ``-u`` or ``--uncheckout`` is passed, this instead deletes the checked out schema (assuming there are no pending
    changes) and removes the HEAD pointer.

    If ``--force`` isn't passed and the schema has pending changes, this will fail.
    """
    repository, image = image_spec
    image = repository.resolve_image(image)

    if uncheckout:
        repository.uncheckout()
        print("Unchecked out %s." % (str(repository),))
    else:
        repository.checkout(image, force=force)
        print("Checked out %s:%s." % (str(repository), image[:12]))


@click.command(name='commit')
@click.argument('repository', type=splitgraph.core.repository.to_repository)
@click.option('-s', '--include-snap', default=False, is_flag=True,
              help='Include the full table snapshots. This consumes more space, '
                   'but makes checkouts faster.')
@click.option('-m', '--message', help='Optional commit message')
def commit_c(repository, include_snap, message):
    """
    Commit changes to a checked-out Splitgraph repository.

    This packages up all changes into a new image. Where a table hasn't been created or had its schema changed,
    this will delta compress the changes. For all other tables (or if ``-s`` has been passed), this will
    store them as full table snapshots.
    """
    new_hash = repository.commit(include_snap=include_snap, comment=message)
    print("Committed %s as %s." % (str(repository), new_hash[:12]))


@click.command(name='tag')
@click.argument('image_spec', type=image_spec_parser(default=None))
@click.argument('tag', required=False)
@click.option('-f', '--force', required=False, is_flag=True, help="Overwrite the tag if it already exists.")
@click.option('-r', '--remove', required=False, is_flag=True, help="Remove the tag instead.")
def tag_c(image_spec, tag, force, remove):
    """
    Manage tags on images.

    Depending on the exact invocation, this command can tag a Splitgraph image,
    list all tags in a repository or delete a tag.

    Examples:

    ``sgr tag noaa/climate``

        List all tagged images in the ``noaa/climate`` repository and their tags.

    ``sgr tag noaa/climate:abcdef1234567890``

        List all tags assigned to the image ``noaa/climate:abcdef1234567890...``

    ``sgr tag noaa/climate:abcdef1234567890 my_new_tag``

        Tag the image ``noaa/climate:abcdef1234567890...`` with ``my_new_tag``. If the tag already exists, this will
        raise an error, unless ``-f`` is passed, which will overwrite the tag.

    ``sgr tag noaa/climate my_new_tag``

        Tag the current ``HEAD`` of ``noaa/climate`` with ``my_new_tag``.

    ``sgr tag --remove noaa/climate:my_new_tag``

        Remove the tag ``my_new_tag`` from ``noaa/climate``.
    """
    repository, image = image_spec

    if remove:
        # In this case the tag must be a part of the image spec.
        if tag is not None or image is None:
            raise click.BadArgumentUsage("Use sgr tag --remove %s:TAG_TO_DELETE" % repository.to_schema())
        if image in ('latest', 'HEAD'):
            raise click.BadArgumentUsage("%s is a reserved tag!" % image)
        repository.get_image(repository.resolve_image(image)).delete_tag(image)
        return

    if tag is None:
        # List all tags
        tag_dict = defaultdict(list)
        for img, img_tag in repository.get_all_hashes_tags():
            tag_dict[img].append(img_tag)
        if image is None:
            for img, tags in tag_dict.items():
                # Sometimes HEAD is none (if we've just cloned the repo)
                if img:
                    print("%s: %s" % (img[:12], ', '.join(tags)))
        else:
            print(', '.join(tag_dict[repository.resolve_image(image)]))
        return

    if tag == 'HEAD':
        raise SplitGraphException("HEAD is a reserved tag!")

    if image is None:
        image = repository.get_head()
    else:
        image = repository.resolve_image(image)

    repository.get_image(image).tag(tag, force)
    print("Tagged %s:%s with %s." % (str(repository), image, tag))


@click.command(name='import')
@click.argument('image_spec', type=image_spec_parser())
@click.argument('table_or_query')
@click.argument('target_repository', type=splitgraph.core.repository.to_repository)
@click.argument('target_table', required=False)
def import_c(image_spec, table_or_query, target_repository, target_table):
    """
    Import tables into a Splitgraph repository.

    Imports a table or a result of a query from a local Splitgraph repository or a Postgres schema into another
    Splitgraph repository.

    Examples:

    ``sgr import noaa/climate:my_tag climate_data my/repository``

        Create a new image in ``my/repository`` with the ``climate_data`` table included. This links the new image to
        the physical object, meaning that the history of the ``climate_data`` table is preserved.

        If no tag is specified, the 'latest' (not the HEAD image or current state of the checked out image)
        image is used.

    ``sgr import noaa/climate:my_tag "SELECT * FROM climate_data" my/repository climate_data``

        Create a new image in ``my/repository`` with the result of the query stored in the ``climate_data`` table. This
        creates a new physical object without any linkage to the original data, so the history of the ``climate_data``
        table isn't preserved. The SQL query can interact with multiple tables in the source image.

    ``sgr import other_schema other_table my/repository``

        Since other_schema isn't a Splitgraph repository, this will copy ``other_schema.other_table``
        into a new Splitgraph object and add the ``other_table`` table to a new image in ``my/repository``.

    Note that importing doesn't discard or commit pending changes in the target Splitgraph repository: a new image
    is created with the new table added, the new table is materialized in the repository and the HEAD pointer is moved.
    """
    repository, image = image_spec

    if repository_exists(repository):
        foreign_table = False
        image = repository.resolve_image(image)
        # If the source table doesn't exist in the image, we'll treat it as a query instead.
        is_query = not bool(repository.get_image(image).get_table(table_or_query))
    else:
        # If the source schema isn't actually a Splitgraph repo, we'll be copying the table verbatim.
        foreign_table = True
        is_query = table_or_query not in repository.engine.get_all_tables(repository.to_schema())
        image = None

    if is_query and not target_table:
        print("TARGET_TABLE is required when the source is a query!")
        sys.exit(1)

    repository.import_tables([table_or_query], target_repository, [target_table] if target_table else [],
                             image_hash=image, foreign_tables=foreign_table,
                             table_queries=[] if not is_query else [True])

    print("%s:%s has been imported from %s:%s%s" % (str(target_repository), target_table, str(repository),
                                                    table_or_query, (' (%s)' % image[:12] if image else '')))
