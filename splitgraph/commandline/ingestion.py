"""Command line tools for ingesting/exporting Splitgraph images into other formats."""
import io
import logging
from itertools import islice

import click
from splitgraph.commandline.common import ImageType, RepositoryType
from splitgraph.core.output import ResettableStream


@click.group(name="csv")
def csv_group():
    """Import/export Splitgraph images in CSV format."""


@click.command(name="export")
@click.argument("image_spec", type=ImageType(default=None))
@click.argument("query")
@click.option(
    "-f",
    "--file",
    type=click.File("w"),
    default="-",
    help="File name to export to, default stdout.",
)
@click.option(
    "-l",
    "--layered",
    help="Don't materialize the tables, use layered querying instead.",
    is_flag=True,
    default=False,
)
def csv_export(image_spec, query, file, layered):
    """
    Export the result of a query as CSV.

    Examples:

    `sgr csv export noaa/climate "SELECT * FROM rainfall"`

    Output everything in the currently checked-out `"rainfall"` table as CSV.

    `sgr csv export noaa/climate:dec_2018 "SELECT * FROM rainfall WHERE state = 'AZ' -f dec_2018_az.csv`

    Check out the `dec_2018` tag of `noaa/climate` and output values from `"rainfall"` for Arizona to `dec_2018_az.csv`

    `sgr csv export --layered noaa/climate:abcdef1234567890 "SELECT * FROM rainfall JOIN other_table ON..."`

    Uses layered querying instead to execute a join on tables in a certain image (satisfying the query without
    having to check the image out).
    """
    from splitgraph.ingestion.csv import csv_adapter

    repository, image = image_spec
    csv_adapter.to_data(query, image, repository, layered, buffer=file)


@click.command(name="import")
@click.argument("repository", type=RepositoryType(exists=True))
@click.argument("table")
@click.option(
    "-f",
    "--file",
    type=click.File("rb"),
    default="-",
    help="File name to import data from, default stdin.",
)
@click.option(
    "-r", "--replace", default=False, is_flag=True, help="Replace the table if it already exists."
)
@click.option(
    "-k",
    "--primary-key",
    multiple=True,
    help="Use the specified column(s) as primary key(s)",
    default=[],
)
@click.option(
    "-t",
    "--override-type",
    multiple=True,
    type=(str, str),
    help="Explicitly set types of these columns to PG types",
)
@click.option("--encoding", help="Encoding to use for the CSV file", default=None)
@click.option("--separator", default=",", help="CSV separator to use")
@click.option(
    "--no-header",
    default=False,
    is_flag=True,
    help="Treats the first line of the CSV as data rather than a header.",
)
@click.option(
    "--skip-schema-check",
    default=False,
    is_flag=True,
    help="Skips checking that the dataframe is compatible with the target schema.",
)
def csv_import(
    repository,
    table,
    file,
    replace,
    primary_key,
    override_type,
    encoding,
    separator,
    no_header,
    skip_schema_check,
):
    """
    Import a CSV file into a checked-out Splitgraph repository. This doesn't create a new image, use `sgr commit`
    after the import and any adjustments (e.g. adding primary keys or converting column types) to do so.

    If the target table doesn't exist, this will create a new table.

    If the target table does exist, this will try and patch the new values in by updating rows that exist in the
    current table (as per its primary key constraints) and inserting new ones. Rows existing in the current table
    but missing in the CSV won't be deleted.

    If `-r` is passed, the table will instead be deleted and recreated from the CSV file if it exists.
    """
    import csv

    from splitgraph.ingestion.csv import csv_adapter
    from splitgraph.ingestion.inference import infer_sg_schema

    if not primary_key:
        click.echo(
            "Warning: primary key is not specified, using the whole row as primary key."
            "This is probably not something that you want."
        )
    stream = ResettableStream(file)
    reader = csv.reader(io.TextIOWrapper(io.BufferedReader(stream)), delimiter=separator or ",")

    # Grab the first few rows from the CSV and give them to TableSchema.
    sample = list(islice(reader, 100))

    if no_header:
        # Patch in a dummy header
        sample = [[str(i) for i in range(len(sample))]] + sample

    type_overrides = dict(override_type or [])
    sg_schema = infer_sg_schema(sample, override_types=type_overrides, primary_keys=primary_key)
    logging.debug("Using Splitgraph schema: %r", sg_schema)

    # Reset the stream and pass it to COPY FROM STDIN
    stream.reset()
    csv_adapter.to_table(
        io.TextIOWrapper(io.BufferedReader(stream)),
        repository,
        table,
        if_exists="replace" if replace else "patch",
        schema_check=not skip_schema_check,
        no_header=no_header,
        delimiter=separator,
        encoding=encoding,
        schema_spec=sg_schema,
    )


csv_group.add_command(csv_export)
csv_group.add_command(csv_import)
