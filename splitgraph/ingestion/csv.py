from typing import Optional, TYPE_CHECKING

from psycopg2.sql import SQL, Identifier

from splitgraph.ingestion.common import IngestionAdapter

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PsycopgEngine


class CSVIngestionAdapter(IngestionAdapter):
    @staticmethod
    def create_ingestion_table(data, engine, schema: str, table: str, **kwargs):
        schema_spec = kwargs.pop("schema_spec")
        engine.delete_table(schema, table)
        engine.create_table(schema, table, schema_spec=schema_spec, include_comments=True)

    @staticmethod
    def data_to_new_table(
        data, engine: "PsycopgEngine", schema: str, table: str, no_header: bool = True, **kwargs
    ):
        copy_csv_buffer(data, engine, schema, table, no_header, **kwargs)

    @staticmethod
    def query_to_data(engine, query: str, schema: Optional[str] = None, **kwargs):
        buffer = kwargs.pop("buffer")
        query_to_csv(engine, query, buffer, schema)
        return buffer


csv_adapter = CSVIngestionAdapter()


def copy_csv_buffer(
    data, engine: "PsycopgEngine", schema: str, table: str, no_header: bool = False, **kwargs
):
    """Copy CSV data from a buffer into a given schema/table"""
    with engine.connection.cursor() as cur:
        extra_args = [not no_header]

        copy_command = SQL("COPY {}.{} FROM STDIN WITH (FORMAT CSV, HEADER %s").format(
            Identifier(schema), Identifier(table)
        )
        for k, v in kwargs.items():
            if k in ("encoding", "delimiter", "escape") and v:
                copy_command += SQL(", " + k + " %s")
                extra_args.append(v)
        copy_command += SQL(")")

        cur.copy_expert(
            cur.mogrify(copy_command, extra_args), data,
        )


def query_to_csv(engine: "PsycopgEngine", query, buffer, schema: Optional[str] = None):
    copy_query = SQL("COPY (") + SQL(query) + SQL(") TO STDOUT WITH (FORMAT CSV, HEADER TRUE);")
    if schema:
        copy_query = SQL("SET search_path TO {},public;").format(Identifier(schema)) + copy_query

    with engine.connection.cursor() as cur:
        cur.copy_expert(copy_query, buffer)
