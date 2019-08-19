"""Command line tools for ingesting/exporting Splitgraph images into other formats."""

import click

from splitgraph.commandline._common import ImageType, RepositoryType


# This commandline entry point doesn't actually import splitgraph.ingestion directly
# as it pulls in Pandas (which can take a second) -- instead it lazily imports it ai
# invocation time and asks the user to install the ingestion extra if Pandas isn't found.


@click.group(name="csv")
def csv():
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
    try:
        from splitgraph.ingestion.pandas import sql_to_df

        repository, image = image_spec
        df = sql_to_df(query, image=image, repository=repository, use_lq=layered)
        df.to_csv(file, index=df.index.names != [None])
    except ImportError:
        print("Install the " "ingestion" " setuptools extra to enable this feature!")
        exit(1)


@click.command(name="import")
@click.argument("repository", type=RepositoryType())
@click.argument("table")
@click.option(
    "-f",
    "--file",
    type=click.File("r"),
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
    default=False,
)
@click.option(
    "-d",
    "--datetime",
    multiple=True,
    help="Try to parse the specified column(s) as timestamps.",
    default=False,
)
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
    repository, table, file, replace, primary_key, datetime, separator, no_header, skip_schema_check
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
    # read_csv is a monster of a function, perhaps we should expose some of its configs here.
    # The reason we don't ingest directly into the engine by using COPY FROM STDIN is so that we can let Pandas do
    # some type inference/preprocessing on the CSV.
    if not primary_key:
        print(
            "Warning: primary key is not specified, using the whole row as primary key."
            "This is probably not something that you want."
        )
    try:
        import pandas as pd
        from splitgraph.ingestion.pandas import df_to_table

        df = pd.read_csv(
            file,
            sep=separator,
            index_col=primary_key,
            parse_dates=list(datetime) if datetime else False,
            infer_datetime_format=True,
            header=(None if no_header else "infer"),
        )
        print("Read %d line(s)" % len(df))
        df_to_table(
            df,
            repository,
            table,
            if_exists="replace" if replace else "patch",
            schema_check=not skip_schema_check,
        )
    except ImportError:
        print("Install the " "ingestion" " setuptools extra to enable this feature!")
        exit(1)


csv.add_command(csv_export)
csv.add_command(csv_import)
