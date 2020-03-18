from abc import abstractmethod
from typing import Optional, Union

from psycopg2.sql import SQL, Identifier

from splitgraph.core.image import Image
from splitgraph.core.repository import Repository
from splitgraph.core.types import TableSchema
from splitgraph.engine.postgres.engine import PsycopgEngine
from splitgraph.exceptions import CheckoutError


def schema_compatible(source_schema: TableSchema, target_schema: TableSchema) -> bool:
    """Quick check to see if a dataframe with target_schema can be written into source_schema.
    There are some implicit type conversions that SQLAlchemy/Pandas can do so we don't want to immediately fail
    if the column types aren't exactly the same (eg bigint vs numeric etc). Most errors should be caught by PG itself.

    Schema is a list of (ordinal, name, type, is_pk).
    """
    if len(source_schema) != len(target_schema):
        return False

    for col1, col2 in zip(sorted(source_schema), sorted(target_schema)):
        # Only check column names
        if col1.name != col2.name:
            return False

    return True


def merge_tables(
    engine: "PsycopgEngine",
    source_schema: str,
    source_table: str,
    source_schema_spec: TableSchema,
    target_schema: str,
    target_table: str,
    target_schema_spec: TableSchema,
):

    # Construct an upsert query: INSERT INTO target_table (cols) (SELECT cols FROM tmp_table) ON CONFLICT (pk_cols)
    #                            DO UPDATE SET (non_pk_cols) = (EXCLUDED.non_pk_cols)
    pk_cols = [c[1] for c in target_schema_spec if c[3]]
    non_pk_cols = [c[1] for c in target_schema_spec if not c[3]]
    # Cast cols for when we're inserting things that Pandas detected as strings into columns with
    # datetimes/ints/etc
    cols_sql = SQL(",").join(Identifier(c[1]) for c in target_schema_spec)
    cols_sql_cast = SQL(",").join(
        Identifier(s[1]) + SQL("::" + t[2]) for s, t in zip(source_schema_spec, target_schema_spec)
    )
    query = (
        SQL("INSERT INTO {}.{} (").format(Identifier(target_schema), Identifier(target_table))
        + cols_sql
        + SQL(") (SELECT ")
        + cols_sql_cast
        + SQL(" FROM {}.{})").format(Identifier(source_schema), Identifier(source_table))
        + SQL(" ON CONFLICT (")
        + SQL(",").join(map(Identifier, pk_cols))
        + SQL(")")
    )
    if non_pk_cols:
        if len(non_pk_cols) > 1:
            query += (
                SQL(" DO UPDATE SET (")
                + SQL(",").join(map(Identifier, non_pk_cols))
                + SQL(") = (")
                + SQL(",").join([SQL("EXCLUDED.") + Identifier(c) for c in non_pk_cols])
                + SQL(")")
            )
        else:
            # otherwise, we get a "source for a multiple-column UPDATE item must be a sub-SELECT or ROW()
            # expression" error from pg.
            query += SQL(" DO UPDATE SET {0} = EXCLUDED.{0}").format(Identifier(non_pk_cols[0]))
    else:
        query += SQL(" DO NOTHING")
    engine.run_sql(query)


class IngestionAdapter:
    @abstractmethod
    def create_ingestion_table(self, data, engine, schema: str, table: str, **kwargs):
        pass

    @abstractmethod
    def data_to_new_table(
        self, data, engine, schema: str, table: str, no_header: bool = True, **kwargs
    ):
        pass

    @abstractmethod
    def query_to_data(self, engine, query: str, schema: Optional[str] = None, **kwargs):
        pass

    def to_table(
        self,
        data,
        repository: "Repository",
        table: str,
        if_exists: str = "patch",
        schema_check: bool = True,
        no_header: bool = False,
        **kwargs
    ):
        tmp_schema = repository.to_schema()

        if not repository.head:
            raise CheckoutError("Repository %s isn't checked out!" % tmp_schema)

        table_exists = repository.object_engine.table_exists(tmp_schema, table)
        if not table_exists or (table_exists and if_exists == "replace"):
            self.create_ingestion_table(data, repository.object_engine, tmp_schema, table, **kwargs)

            if if_exists == "replace":
                # Currently, if a table is dropped and recreated with the same schema,
                # Splitgraph has no way of finding that out. So what we do is drop the last column
                # in the schema and then create it again with the same type: this increments the
                # ordinal on the column, which Splitgraph detects as a schema change.
                #
                # This is a (very ugly) hack but the root cause fix is not straightforward.

                table_schema = repository.object_engine.get_full_table_schema(tmp_schema, table)
                repository.object_engine.run_sql_in(
                    tmp_schema,
                    SQL(
                        "ALTER TABLE {0} DROP COLUMN {1};ALTER TABLE {0} ADD COLUMN {1} %s"
                        % table_schema[-1].pg_type
                    ).format(Identifier(table), Identifier(table_schema[-1].name)),
                )

            self.data_to_new_table(
                data, repository.object_engine, tmp_schema, table, no_header, **kwargs
            )
            repository.commit_engines()
            return

        # If we've reached this point, the table exists and we're patching values into it.
        # Ingest the table into a temporary location.
        tmp_table = "sg_tmp_ingestion" + table
        self.create_ingestion_table(data, repository.object_engine, tmp_schema, tmp_table, **kwargs)

        try:
            source_schema = repository.engine.get_full_table_schema(tmp_schema, tmp_table)
            target_schema = repository.engine.get_full_table_schema(tmp_schema, table)
            if schema_check and not schema_compatible(source_schema, target_schema):
                raise ValueError(
                    "Schema changes are unsupported with if_exists='patch'!"
                    "\nSource schema: %r\nTarget schema: %r" % (source_schema, target_schema)
                )

            self.data_to_new_table(
                data, repository.engine, tmp_schema, tmp_table, no_header=no_header
            )

            merge_tables(
                repository.engine,
                source_schema=tmp_schema,
                source_table=tmp_table,
                source_schema_spec=source_schema,
                target_schema=tmp_schema,
                target_table=table,
                target_schema_spec=target_schema,
            )
            repository.commit_engines()
        finally:
            repository.engine.delete_table(tmp_schema, tmp_table)

    def to_data(
        self,
        query: str,
        image: Optional[Union[Image, str]] = None,
        repository: Optional[Repository] = None,
        use_lq: bool = False,
        **kwargs
    ):
        if image is None:
            if repository is None:
                raise ValueError("repository must be set!")
            # Run the query against the current staging area.
            return self.query_to_data(
                repository.object_engine, query, repository.to_schema(), **kwargs
            )

        # Otherwise, check the image out (full or temporary LQ). Corner case here to fix in the future:
        # if the image is the same as current HEAD and there are no changes, there's no need to do a check out.
        if isinstance(image, str):
            if repository is None:
                raise ValueError("repository must be set!")
            image = repository.images[image]

        if not use_lq:
            image.checkout(force=False)  # Make sure to fail if we have pending changes.
            return self.query_to_data(image.engine, query, image.repository.to_schema(), **kwargs)

        # If we're using LQ, then run the query against a tmp schema
        # (won't download objects unless needed).
        with image.query_schema() as tmp_schema:
            return self.query_to_data(image.engine, query, tmp_schema, **kwargs)
