import sys
from collections import defaultdict

import click

import splitgraph as sg
from splitgraph.commandline.common import parse_image_spec


@click.command(name='checkout')
@click.argument('image_spec', type=parse_image_spec)
def checkout_c(image_spec):
    """
    Checks out a Splitgraph image into a Postgres schema, discarding all pending changes, downloading the required
    objects and materializing all tables.

    Image spec must be of the format [NAMESPACE/]REPOSITORY[:HASH_OR_TAG]. Note that currently, the schema that the
    image is checked out into has to have the same name as the repository.
    """
    repository, image = image_spec
    image = sg.resolve_image(repository, image)
    sg.checkout(repository, image)
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


# TODO we can detect foreign tables ourselves -- in the case where we treated something as an SG image and instead
# would like to treat it as a full table, we can just do SELECT *.
@click.command(name='import')
@click.argument('repository', type=sg.to_repository)
@click.argument('table_or_query')
@click.argument('target_repository', type=sg.to_repository)
@click.argument('target_table', required=False)
@click.argument('image', required=False)
@click.option('-q', '--is-query', is_flag=True, default=False)
@click.option('-f', '--foreign-tables', is_flag=True, default=False)
def import_c(repository, table_or_query, target_repository, target_table,
             image, is_query, foreign_tables):  # pylint disable=missing-docstring
    if is_query and not target_table:
        print("TARGET_TABLE is required when is_query is True!")
        sys.exit(1)

    if not foreign_tables:
        if not image:
            image = sg.get_current_head(repository)
        else:
            image = sg.get_canonical_image_id(repository, image)
    else:
        image = None
    sg.import_tables(repository, [table_or_query], target_repository, [target_table] if target_table else [],
                     image_hash=image, foreign_tables=foreign_tables, table_queries=[] if not is_query else [True])

    print("%s:%s has been imported from %s:%s%s" % (str(target_repository), target_table, str(repository),
                                                    table_or_query, (' (%s)' % image[:12] if image else '')))
