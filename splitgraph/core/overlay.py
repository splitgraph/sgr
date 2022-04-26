from typing import TYPE_CHECKING

from psycopg2.sql import SQL, Identifier

from splitgraph.core.types import TableSchema

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine

SG_ROW_SEQ = "_sg_row_seq"
WRITE_LOWER_PREFIX = "_sgov_lower_"
WRITE_UPPER_PREFIX = "_sgov_upper_"
WRITE_MERGED_PREFIX = "_sgov_merged_"


def init_write_overlay(
    object_engine: "PostgresEngine",
    schema: str,
    table: str,
    table_schema: TableSchema,
    ddn_layout: bool = False,
) -> None:
    from splitgraph.engine.postgres.engine import SG_UD_FLAG

    upper_table = WRITE_UPPER_PREFIX + table
    # LQ checkout with sgr vs DDN differ in that in the former case the user
    # interacts with the overlay view directly, and can thus see the all pending
    # changes at any time. In the latter case, the user interacts only with the
    # foreign table backed by LQFDW, and the writes are not visible until flushed.
    lower_table = table if ddn_layout else WRITE_LOWER_PREFIX + table
    overlay_view = WRITE_MERGED_PREFIX + table if ddn_layout else table

    # Create the "upper" table that actual writes will be recorded in (staging area for
    # new objects)
    object_engine.run_sql(
        SQL(
            "DROP TABLE IF EXISTS {0}.{1};"
            "CREATE UNLOGGED TABLE {0}.{1} (LIKE {0}.{2});"
            "ALTER TABLE {0}.{1} ADD COLUMN {3} BOOLEAN DEFAULT FALSE;"
            "ALTER TABLE {0}.{1} ADD COLUMN {4} SERIAL;"
        ).format(
            Identifier(schema),
            Identifier(upper_table),
            Identifier(lower_table),
            Identifier(SG_UD_FLAG),
            Identifier(SG_ROW_SEQ),
        )
    )

    pk_cols, _ = object_engine.schema_spec_to_cols(table_schema)
    pk_cols_s = SQL(",").join(map(Identifier, pk_cols))
    all_cols = SQL(",").join([Identifier(column.name) for column in table_schema])

    # Create a view to see the latest writes on reads to uncommitted images
    object_engine.run_sql(
        SQL(
            """DROP VIEW IF EXISTS {schema}.{table};
            CREATE VIEW {schema}.{table} AS
            WITH _sg_flattened AS (
                SELECT DISTINCT ON ({pks})
                    {all_cols}, {sg_ud_flag}
                FROM {schema}.{upper}
                ORDER BY {pks}, {sg_row_seq} DESC
            )

            -- Include rows from the base table that aren't overwritten
            SELECT {all_cols} FROM {schema}.{lower}
            WHERE ({pks}) NOT IN (
                SELECT {pks} FROM {schema}.{upper}
            )

            UNION ALL

            -- Include all the upserted rows
            SELECT {all_cols} FROM _sg_flattened
            WHERE {sg_ud_flag} IS TRUE
        """
        ).format(
            schema=Identifier(schema),
            table=Identifier(overlay_view),
            pks=pk_cols_s,
            all_cols=all_cols,
            lower=Identifier(lower_table),
            upper=Identifier(upper_table),
            sg_row_seq=Identifier(SG_ROW_SEQ),
            sg_ud_flag=Identifier(SG_UD_FLAG),
        )
    )

    if not ddn_layout:
        # Transfer comment from original table to the view
        query = SQL("")
        args = []
        for col in table_schema:
            if col.comment:
                query += SQL("COMMENT ON COLUMN {}.{}.{} IS %s;").format(
                    Identifier(schema), Identifier(overlay_view), Identifier(col.name)
                )
                args.append(col.comment)
        if len(args) > 0:
            object_engine.run_sql(query, args)

    # Create a trigger
    object_engine.run_sql(
        SQL(
            """CREATE OR REPLACE FUNCTION {0}.{1}()
    RETURNS TRIGGER
    AS $$
BEGIN
    IF OLD IS NOT NULL AND NEW IS NULL THEN
        INSERT INTO {0}.{2} VALUES (OLD.*, FALSE);
    ELSIF OLD IS NOT NULL AND NEW IS NOT NULL THEN
        INSERT INTO {0}.{2} VALUES (OLD.*, FALSE);
        INSERT INTO {0}.{2} VALUES (NEW.*, TRUE);
    ELSIF OLD IS NULL AND NEW IS NOT NULL THEN
        INSERT INTO {0}.{2} VALUES (NEW.*, TRUE);
    END IF;

    IF NEW IS NOT NULL THEN
        RETURN NEW;
    END IF;
    RETURN OLD;
END
$$
LANGUAGE plpgsql;
CREATE TRIGGER {1} INSTEAD OF INSERT OR UPDATE OR DELETE ON {0}.{1}
    FOR EACH ROW EXECUTE FUNCTION {0}.{1}();
"""
        ).format(
            Identifier(schema),
            Identifier(overlay_view),
            Identifier(upper_table),
        )
    )
