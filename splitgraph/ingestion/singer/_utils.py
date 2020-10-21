import logging
import traceback
from functools import wraps

from psycopg2.sql import SQL, Identifier

from splitgraph.engine import validate_type


def log_exception(f):
    """Emit exceptions with full traceback instead of just the error text"""

    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception:
            logging.error(traceback.format_exc())
            raise

    return wrapped


def _migrate_schema(engine, table_schema, table_name, table_schema_spec, new_schema_spec):
    """Migrate the schema of a table to match the schema_spec"""

    old_cols = {c.name: c.pg_type for c in table_schema_spec}
    new_cols = {c.name: c.pg_type for c in new_schema_spec}
    for c in old_cols:
        if c not in new_cols:
            engine.run_sql(
                SQL("ALTER TABLE {}.{} DROP COLUMN {}").format(
                    Identifier(table_schema), Identifier(table_name), Identifier(c),
                )
            )
    for c in new_cols:
        if c not in old_cols:
            engine.run_sql(
                SQL("ALTER TABLE {}.{} ADD COLUMN {} %s" % validate_type(new_cols[c])).format(
                    Identifier(table_schema), Identifier(table_name), Identifier(c),
                )
            )
        elif new_cols[c] != old_cols[c]:
            engine.run_sql(
                SQL(
                    "ALTER TABLE {}.{} ALTER COLUMN {} TYPE %s" % validate_type(new_cols[c])
                ).format(
                    Identifier(table_schema), Identifier(table_name), Identifier(c),
                )
            )
