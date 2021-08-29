"""
sgr commands related to creating and checking out images
"""
import sys
from collections import defaultdict

import click
from splitgraph.commandline.common import (
    ImageType,
    JsonType,
    RepositoryType,
    remote_switch_option,
)
from splitgraph.config import DEFAULT_CHUNK_SIZE
from splitgraph.exceptions import TableNotFoundError


@click.command(name="checkout")
@click.argument("image_spec", type=ImageType(default="HEAD", get_image=True))
@click.option(
    "-f", "--force", help="Discard all pending changes to the schema", is_flag=True, default=False
)
@click.option(
    "-u", "--uncheckout", help="Delete the checked out copy instead", is_flag=True, default=False
)
@click.option(
    "-l",
    "--layered",
    help="Don't materialize the tables, use layered querying instead.",
    is_flag=True,
    default=False,
)
def checkout_c(image_spec, force, uncheckout, layered):
    """
    Check out a Splitgraph image into a Postgres schema.

    This downloads the required physical objects and materializes all tables, unless ``-l`` or ``--layered`` is passed,
    in which case the objects are downloaded and a foreign data wrapper is set up on the engine to satisfy read-only
    queries by combining results from each table's fragments.

    Tables checked out in this way are still presented as normal Postgres tables and can queried in the same way.
    Since the tables aren't materialized, layered querying is faster to set up, but since each query now results in a
    subquery to each object comprising the table, actual query execution is slower than to materialized Postgres tables.

    Layered querying is only supported for read-only queries.

    Image spec must be of the format ``[NAMESPACE/]REPOSITORY[:HASH_OR_TAG]``. Note that currently, the schema that the
    image is checked out into has to have the same name as the repository. If no image hash or tag is passed,
    "HEAD" is assumed.

    If ``-u`` or ``--uncheckout`` is passed, this instead deletes the checked out schema (assuming there are no pending
    changes) and removes the HEAD pointer.

    If ``--force`` isn't passed and the schema has pending changes, this will fail.
    """
    repository, image = image_spec

    if uncheckout:
        repository.uncheckout(force=force)
        click.echo("Unchecked out %s." % (str(repository),))
    else:
        image.checkout(force=force, layered=layered)
        click.echo("Checked out %s:%s." % (str(repository), image.image_hash[:12]))


@click.command(name="commit")
@click.argument("repository", type=RepositoryType(exists=True))
@click.option(
    "-s",
    "--snap",
    default=False,
    is_flag=True,
    help="Do not delta compress the changes and instead store the whole table again. "
    "This consumes more space, but makes checkouts faster.",
)
@click.option(
    "-c",
    "--chunk-size",
    default=DEFAULT_CHUNK_SIZE,
    type=int,
    help="Split new tables into chunks of this many rows (by primary key). The default "
    "value is governed by the SG_COMMIT_CHUNK_SIZE configuration parameter.",
)
@click.option(
    "-k",
    "--chunk-sort-keys",
    default=None,
    type=JsonType(),
    help="Sort the data inside each chunk by this/these key(s)",
)
@click.option(
    "-t",
    "--split-changesets",
    default=False,
    is_flag=True,
    help="Split changesets for existing tables across original chunk boundaries.",
)
@click.option(
    "-i",
    "--index-options",
    type=JsonType(),
    help="JSON dictionary of extra indexes to calculate on the new objects.",
)
@click.option("-m", "--message", help="Optional commit message")
@click.option(
    "-o", "--overwrite", is_flag=True, help="Overwrite physical objects that already exist"
)
def commit_c(
    repository,
    snap,
    chunk_size,
    chunk_sort_keys,
    split_changesets,
    index_options,
    message,
    overwrite,
):
    """
    Commit changes to a checked-out Splitgraph repository.

    This packages up all changes into a new image. Where a table hasn't been created or had its schema changed,
    this will delta compress the changes. For all other tables (or if ``-s`` has been passed), this will
    store them as full table snapshots.

    When a table is stored as a full snapshot, `--chunk-size` sets the maximum size, in rows, of the fragments
    that the table will be split into (default is no splitting). The splitting is done by the
    table's primary key.

    If `--split-changesets` is passed, delta-compressed changes will also be split up according to the original
    table chunk boundaries. For example, if there's a change to the first and the 20000th row of a table that was
    originally committed with `--chunk-size=10000`, this will create 2 fragments: one based on the first chunk
    and one on the second chunk of the table.

    If `--chunk-sort-keys` is passed, data inside the chunk is sorted by this key (or multiple keys).
    This helps speed up queries on those keys for storage layers than can leverage that (e.g. CStore). The expected format is JSON, e.g. `{table_1: [col_1, col_2]}`

    `--index-options` expects a JSON-serialized dictionary of `{table: index_type: column: index_specific_kwargs}`.
    Indexes are used to narrow down the amount of chunks to scan through when running a query. By default, each column
    has a range index (minimum and maximum values) and it's possible to add bloom filtering to speed up queries that
    involve equalities.

    Bloom filtering allows to trade off between the space overhead of the index and the probability of a false
    positive (claiming that an object contains a record when it actually doesn't, leading to extra scans).

    An example `index-options` dictionary:

    \b
    ```
    {
        "table": {
            "bloom": {
                "column_1": {
                    "probability": 0.01,   # Only one of probability
                    "size": 10000          # or size can be specified.
                }
            },
            # Only compute the range index on these columns. By default,
            # it's computed on all columns and is always computed on the
            # primary key no matter what.
            "range": ["column_2", "column_3"]
        }
    }
    ```
    """
    new_hash = repository.commit(
        comment=message,
        snap_only=snap,
        chunk_size=chunk_size,
        split_changeset=split_changesets,
        extra_indexes=index_options,
        in_fragment_order=chunk_sort_keys,
        overwrite=overwrite,
    ).image_hash
    click.echo("Committed %s as %s." % (str(repository), new_hash[:12]))


@click.command(name="tag")
@click.argument("image_spec", type=ImageType(default=None))
@click.argument("tag", required=False)
@click.option("-d", "--delete", is_flag=True, help="Delete the tag instead.")
@remote_switch_option()
def tag_c(image_spec, tag, delete):
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
    overwrite the tag.

    ``sgr tag noaa/climate my_new_tag``

    Tag the current ``HEAD`` of ``noaa/climate`` with ``my_new_tag``.

    ``sgr tag --delete noaa/climate:my_new_tag``

    Delete the tag ``my_new_tag`` from ``noaa/climate``.
    """
    repository, image = image_spec

    if delete:
        # In this case the tag must be a part of the image spec.
        if tag is not None or image is None:
            raise click.BadArgumentUsage(
                "Use sgr tag --delete %s:TAG_TO_DELETE" % repository.to_schema()
            )
        if image in ("latest", "HEAD"):
            raise click.BadArgumentUsage("%s is a reserved tag!" % image)
        repository.images[image].delete_tag(image)
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
                    click.echo("%s: %s" % (img[:12], ", ".join(sorted(tags))))
        else:
            click.echo(", ".join(tag_dict[repository.images[image].image_hash]))
        return

    if tag == "HEAD":
        raise click.BadArgumentUsage("HEAD is a reserved tag!")

    if image is None:
        image = repository.head
    else:
        image = repository.images[image]

    image.tag(tag)
    click.echo("Tagged %s:%s with %s." % (str(repository), image.image_hash, tag))


@click.command(name="import")
@click.argument("image_spec", type=ImageType())
@click.argument("table_or_query")
@click.argument("target_repository", type=RepositoryType())
@click.argument("target_table", required=False)
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
    from splitgraph.core.engine import repository_exists

    repository, image = image_spec

    if repository_exists(repository):
        foreign_table = False
        image = repository.images[image]
        # If the source table doesn't exist in the image, we'll treat it as a query instead.
        try:
            image.get_table(table_or_query)
            is_query = False
        except TableNotFoundError:
            is_query = True
    else:
        # If the source schema isn't actually a Splitgraph repo, we'll be copying the table verbatim.
        foreign_table = True
        is_query = table_or_query not in repository.engine.get_all_tables(repository.to_schema())
        image = None

    if is_query and not target_table:
        click.echo("TARGET_TABLE is required when the source is a query!")
        sys.exit(1)

    target_repository.import_tables(
        [target_table] if target_table else [],
        repository,
        [table_or_query],
        image_hash=image.image_hash if image else None,
        foreign_tables=foreign_table,
        table_queries=[] if not is_query else [True],
    )

    click.echo(
        "%s:%s has been imported from %s:%s%s"
        % (
            str(target_repository),
            target_table,
            str(repository),
            table_or_query,
            (" (%s)" % image.image_hash[:12] if image else ""),
        )
    )


@click.command(name="reindex")
@click.argument("image_spec", type=ImageType(default="HEAD", get_image=True))
@click.argument("table_name", type=str)
@click.option(
    "-i",
    "--index-options",
    type=JsonType(),
    required=True,
    help="JSON dictionary of extra indexes to calculate, e.g. "
    '\'{"bloom": {"column_1": {"probability": 0.01}}}\'',
)
@click.option(
    "-o",
    "--ignore-patch-objects",
    type=bool,
    is_flag=True,
    default=False,
    help="Ignore objects that change other objects' rows instead of raising an error",
)
def reindex_c(image_spec, table_name, index_options, ignore_patch_objects):
    """
    Run extra indexes on a table. This will merge the indexing results for all objects
    that a table is formed from with the current object indexes. For explanation of
    what indexes do, see the documentation for `sgr commit`.

    If the objects haven't been downloaded yet, this will download them.

    Currently reindexing objects that change other objects is unsupported and will raise
    an error. Pass `-o` to ignore these objects and only reindex supported objects.

    Image spec must be of the format ``[NAMESPACE/]REPOSITORY[:HASH_OR_TAG]``. If no tag is specified, ``HEAD`` is used.
    """
    from splitgraph.core.output import pluralise

    repository, image = image_spec
    table = image.get_table(table_name)
    click.echo("Reindexing table %s:%s/%s" % (repository.to_schema(), image.image_hash, table_name))
    reindexed = table.reindex(
        extra_indexes=index_options, raise_on_patch_objects=not ignore_patch_objects
    )
    click.echo("Reindexed %s" % pluralise("object", len(reindexed)))
