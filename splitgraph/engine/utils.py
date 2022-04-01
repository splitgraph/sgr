from typing import TYPE_CHECKING

from psycopg2.sql import SQL, Identifier

if TYPE_CHECKING:
    from splitgraph.engine.postgres.psycopg import PsycopgEngine


def unmount_schema(engine: "PsycopgEngine", schema: str) -> None:
    engine.run_sql(
        SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier("%s_lq_checkout_server" % schema))
    )
    engine.run_sql(SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(schema + "_server")))
    engine.run_sql(SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(schema)))
