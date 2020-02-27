"""
Command line routines generating example data / Splitfiles
"""
from hashlib import sha256
from typing import TYPE_CHECKING

import click

from splitgraph.commandline.common import RepositoryType
from splitgraph.core.types import TableColumn

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository

_DEMO_TABLE_SIZE = 10
_DEMO_CHANGE_SIZE = 2
_DEMO_TEMPLATE = """# Import the table from the latest image in the first repository
FROM {repo_1} IMPORT {table_1} AS table_1

# Import the table from a certain image (passed in as a parameter) in the second repository
FROM {repo_2}:${{IMAGE_2}} IMPORT {table_2} AS table_2

# Create a join table
SQL CREATE TABLE result AS SELECT table_1.key, table_1.value AS value_1,\\
                                  table_2.value AS value_2\\
                           FROM table_1 JOIN table_2\\
                           ON table_1.key = table_2.key 
"""


@click.group(name="example")
def example():
    """Generate demo Splitgraph data."""


def _hash(val: int) -> str:
    """Use deterministic values to showcase reusing fragments."""
    return sha256(str(val).encode("ascii")).hexdigest()


def generate_table(repository: "Repository", table_name: str, size: int) -> None:
    """
    Creates a table with an integer primary key and a string value.

    :param repository: Checked-out Repository to create the table in.
    :param table_name: Name of the table to generate
    :param size: Number of rows in the table.
    """
    from psycopg2.sql import SQL
    from psycopg2.sql import Identifier

    repository.engine.create_table(
        repository.to_schema(),
        table_name,
        [
            TableColumn(1, "key", "integer", True, "Some key"),
            TableColumn(2, "value", "varchar", False, "Some value"),
        ],
    )
    repository.engine.run_sql_batch(
        SQL("INSERT INTO {} VALUES (%s, %s)").format(Identifier(table_name)),
        [(i, _hash(i)) for i in range(size)],
        schema=repository.to_schema(),
    )


def alter_table(
    repository: "Repository", table_name: str, rows_added: int, rows_deleted: int, rows_updated: int
) -> None:
    """
    Alters the example table, adding/updating/deleting a certain number of rows.

    :param repository: Checked-out Repository object.
    :param table_name: Name of the table
    :param rows_added: Number of rows to add
    :param rows_deleted: Number of rows to remove
    :param rows_updated: Number of rows to update
    """
    from splitgraph.engine import ResultShape
    from splitgraph.core.sql import select
    from psycopg2.sql import Identifier, SQL

    keys = repository.run_sql(
        select(table_name, "key", schema=repository.to_schema()), return_shape=ResultShape.MANY_ONE
    )
    last = repository.run_sql(
        select(table_name, "MAX(key)", schema=repository.to_schema()),
        return_shape=ResultShape.ONE_ONE,
    )

    # Delete first N rows
    click.echo("Deleting %d rows..." % rows_deleted)
    repository.engine.run_sql_batch(
        SQL("DELETE FROM {} WHERE key = %s").format(Identifier(table_name)),
        [(k,) for k in keys[:rows_deleted]],
        schema=repository.to_schema(),
    )

    # Update next N rows
    click.echo("Updating %d rows..." % rows_updated)
    repository.engine.run_sql_batch(
        SQL("UPDATE {} SET value = %s WHERE key = %s").format(Identifier(table_name)),
        [(_hash(k) + "_UPDATED", k) for k in keys[rows_updated : rows_updated * 2]],
        schema=repository.to_schema(),
    )

    # Insert rows at the end
    click.echo("Adding %d rows..." % rows_added)
    repository.engine.run_sql_batch(
        SQL("INSERT INTO {} VALUES (%s, %s)").format(Identifier(table_name)),
        [(k, _hash(k)) for k in range(last + 1, last + rows_added + 1)],
        schema=repository.to_schema(),
    )


@click.command(name="generate")
@click.argument("repository", type=RepositoryType())
def generate_c(repository):
    """
    Generate a repository with some example data.

    :param repository: Repository to generate. Must not already exist.
    """
    from splitgraph.core.engine import repository_exists

    if repository_exists(repository):
        raise click.ClickException(
            "Repository %s already exists, use sgr rm to delete it!" % repository.to_schema()
        )

    repository.init()
    # Insert some data
    generate_table(repository, "demo", size=_DEMO_TABLE_SIZE)

    image = repository.commit()
    click.echo(
        "Generated %s:%s with %s rows, image hash %s."
        % (repository.to_schema(), "demo", _DEMO_TABLE_SIZE, image.image_hash[:12])
    )


@click.command(name="alter")
@click.argument("repository", type=RepositoryType(exists=True))
def alter_c(repository):
    """
    Alter the table in an example repository.

    This deletes the first 2 rows, updates the next 2 rows and adds 2 rows at the end. Doesn't create a new image.

    :param repository: Generated demo repository.
    """
    alter_table(repository, "demo", _DEMO_CHANGE_SIZE, _DEMO_CHANGE_SIZE, _DEMO_CHANGE_SIZE)


@click.command(name="splitfile")
@click.argument("repository_1", type=RepositoryType())
@click.argument("repository_2", type=RepositoryType())
def splitfile_c(repository_1, repository_2):
    """
    Generate a sample Splitfile.

    The Splitfile imports tables from two example repositories (generated by sgr generate/alter) and runs a
    JOIN query on them.

    :param repository_1: First repository
    :param repository_2: Second repository
    """
    click.echo(
        _DEMO_TEMPLATE.format(
            repo_1=repository_1.to_schema(),
            repo_2=repository_2.to_schema(),
            table_1="demo",
            table_2="demo",
        )
    )


example.add_command(generate_c)
example.add_command(alter_c)
example.add_command(splitfile_c)
