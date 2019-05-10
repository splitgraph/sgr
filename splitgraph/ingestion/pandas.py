"""Routines that ingest/export CSV files to/from Splitgraph images using Pandas"""

from io import StringIO

import pandas as pd
from pandas.io.sql import get_schema
from psycopg2.sql import Identifier, SQL
from splitgraph.core.image import Image
from splitgraph.exceptions import CheckoutError
from sqlalchemy import create_engine


def _get_sqlalchemy_engine(engine):
    server, port, username, password, dbname = (
        engine.conn_params["SG_ENGINE_HOST"],
        engine.conn_params["SG_ENGINE_PORT"],
        engine.conn_params["SG_ENGINE_USER"],
        engine.conn_params["SG_ENGINE_PWD"],
        engine.conn_params["SG_ENGINE_DB_NAME"],
    )
    return create_engine("postgresql://%s:%s@%s:%s/%s" % (username, password, server, port, dbname))


# Internally, we use Pandas' `read_sql_table/query` because they have really good type inference via SQLAlchemy.
# It will be much faster to use the CSV trick but then we'll have to infer and coerce the types manually.


def _table_to_df(engine, schema, table, **kwargs):
    return pd.read_sql_table(
        table_name=table, con=_get_sqlalchemy_engine(engine), schema=schema, **kwargs
    )


def _sql_to_df(engine, sql, schema, **kwargs):
    return pd.read_sql_query(
        sql=SQL("SET search_path TO {};").format(Identifier(schema)).as_string(engine.connection)
        + sql,
        con=_get_sqlalchemy_engine(engine),
        **kwargs
    )


def sql_to_df(sql, image=None, repository=None, use_lq=False, **kwargs):
    """
    Executes an SQL query against a Splitgraph image, returning the result.

    Extra `**kwargs` are passed to Pandas' `read_sql_query`.

    :param sql: SQL query to execute.
    :param image: Image object, image hash/tag (`str`) or None (use the currently checked out image).
    :param repository: Repository the image belongs to. Must be set if `image` is a hash/tag or None.
    :param use_lq: Whether to use layered querying or check out the image if it's not checked out.
    :return: A Pandas dataframe.
    """

    # Initial parameter validation
    if not isinstance(image, Image) and not repository:
        raise ValueError("repository must be set!")

    if image is None:
        # Run the query against the current staging area.
        return _sql_to_df(
            engine=repository.engine, sql=sql, schema=repository.to_schema(), **kwargs
        )

    # Otherwise, check the image out (full or temporary LQ). Corner case here to fix in the future:
    # if the image is the same as current HEAD and there are no changes, there's no need to do a check out.
    if isinstance(image, str):
        image = repository.images[image]

    if not use_lq:
        image.checkout(force=False)  # Make sure to fail if we have pending changes.
        return _sql_to_df(
            engine=image.engine, sql=sql, schema=image.repository.to_schema(), **kwargs
        )

    # If we're using LQ, then run the query against a tmp schema (won't download objects unless needed).
    with image.query_schema() as tmp_schema:
        return _sql_to_df(engine=image.engine, sql=sql, schema=tmp_schema, **kwargs)


def _df_to_empty_table(engine, df, target_schema, target_table, use_ordinal_hack=False):
    # Use sqlalchemy's engine to convert types and create a DDL statement for the table.

    # If there's an unnamed index (created by Pandas), we don't add PKs to the table.
    if df.index.names == [None]:
        ddl = get_schema(df, name=target_table, con=_get_sqlalchemy_engine(engine))
    else:
        ddl = get_schema(
            df.reset_index(),
            name=target_table,
            keys=df.index.names,
            con=_get_sqlalchemy_engine(engine),
        )
    engine.run_sql_in(target_schema, ddl)

    if use_ordinal_hack:
        # Currently, if a table is dropped and recreated with the same schema, Splitgraph has no way of
        # finding that out (see test/splitgraph/commands/test_commit_diff.py::test_drop_recreate_produces_snap in
        # the main splitgraph repo). So what we do is drop the last column in the schema and then create it again with
        # the same type: this increments the ordinal on the column, which Splitgraph detects as a schema change.
        #
        # This is a (very ugly) hack but the root cause fix is not straightforward (see the comment on the test).
        table_schema = engine.get_full_table_schema(target_schema, target_table)
        _, cname, ctype, _ = table_schema[-1]
        engine.run_sql_in(
            target_schema,
            SQL("ALTER TABLE {0} DROP COLUMN {1};ALTER TABLE {0} ADD COLUMN {1} %s" % ctype).format(
                Identifier(target_table), Identifier(cname)
            ),
        )

    engine.commit()


def _df_to_table_fast(engine, df, target_schema, target_table):
    # Instead of using Pandas' to_sql, dump the dataframe to csv and then load it on the other
    # end using Psycopg's copy_to.
    with engine.connection.cursor() as cur:
        buffer = StringIO()
        # Don't write the index column if it's unnamed (generated by Pandas)
        df.to_csv(buffer, header=False, sep="\t", index=df.index.names != [None])
        buffer.seek(0)
        cur.copy_from(
            buffer,
            SQL("{}.{}").format(Identifier(target_schema), Identifier(target_table)).as_string(cur),
            null="",
        )
    engine.commit()


def _schema_compatible(source_schema, target_schema):
    """Quick check to see if a dataframe with target_schema can be written into source_schema.
    There are some implicit type conversions that SQLAlchemy/Pandas can do so we don't want to immediately fail
    if the column types aren't exactly the same (eg bigint vs numeric etc). Most errors should be caught by PG itself.

    Schema is a list of (ordinal, name, type, is_pk).
    """
    if len(source_schema) != len(target_schema):
        return False

    for col1, col2 in zip(sorted(source_schema), sorted(target_schema)):
        # Only check column names
        if col1[1] != col2[1]:
            return False

    return True


def df_to_table(df, repository, table, if_exists="patch"):
    """Writes a Pandas DataFrame to a checked-out Splitgraph table. Doesn't create a new image.

    :param df: Pandas DataFrame to insert.
    :param repository: Splitgraph Repository object. Must be checked out.
    :param table: Table name.
    :param if_exists: Behaviour if the table already exists: 'patch' means that primary keys that already exist in the
    table will be updated and ones that don't will be inserted. 'replace' means that the table will be dropped and
    recreated.
    """

    schema = repository.to_schema()

    if not repository.head:
        raise CheckoutError("Repository %s isn't checked out!" % schema)

    table_exists = repository.engine.table_exists(schema, table)
    if not table_exists or (table_exists and if_exists == "replace"):
        if if_exists == "replace":
            repository.engine.delete_table(schema, table)
        _df_to_empty_table(
            repository.engine, df, schema, table, use_ordinal_hack=if_exists == "replace"
        )
        _df_to_table_fast(repository.engine, df, schema, table)
        repository.commit_engines()
        return

    # If we've reached this point, the table exists and we're patching values into it. Ingest the table
    # into a temporary location.

    # Gotcha here: if we use a 63-character identifier here, then the DDL created by SQLAlchemy will have
    # a 66-character constraint in it (name + "_pk") which will clash with the name of the table itself.
    tmp_table = "sg_tmp_ingestion" + table

    # We use SQLAlchemy to create the empty table. Verify that the table schema is compatible.
    _df_to_empty_table(repository.engine, df, schema, tmp_table)

    try:

        source_schema = repository.engine.get_full_table_schema(schema, tmp_table)
        target_schema = repository.engine.get_full_table_schema(schema, table)

        if not _schema_compatible(source_schema, target_schema):
            raise ValueError(
                "Schema changes are unsupported with if_exists='patch'!"
                "\nSource schema: %r\nTarget schema: %r" % (source_schema, target_schema)
            )

        _df_to_table_fast(repository.engine, df, schema, tmp_table)

        # Construct an upsert query: INSERT INTO target_table (cols) (SELECT cols FROM tmp_table) ON CONFLICT (pk_cols)
        #                            DO UPDATE SET (non_pk_cols) = (EXCLUDED.non_pk_cols)
        pk_cols = [c[1] for c in target_schema if c[3]]
        non_pk_cols = [c[1] for c in target_schema if not c[3]]

        query = SQL("INSERT INTO {0}.{1} (SELECT * FROM {0}.{2})").format(
            Identifier(schema), Identifier(table), Identifier(tmp_table)
        )
        query += SQL(" ON CONFLICT (") + SQL(",").join(map(Identifier, pk_cols)) + SQL(")")
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

        repository.engine.run_sql(query)
        repository.commit_engines()
    finally:
        repository.engine.delete_table(schema, tmp_table)
