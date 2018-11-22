import json
import logging

from psycopg2.sql import SQL, Identifier
from splitgraph.constants import SPLITGRAPH_META_SCHEMA
from splitgraph.pg_utils import get_full_table_schema, create_table


# Utilities to dump objects (SNAP/DIFF) into an external format.
# We use a slightly ad hoc format: the schema (JSON) + a null byte + Postgres's copy_to
# binary format (only contains data). There's probably some scope to make this more optimized, maybe
# we should look into columnar on-disk formats (Parquet/Avro) but we currently just want to get the objects
# out of/into postgres as fast as possible.

def dump_object_to_file(conn, object_id, path):
    with open(path, 'wb') as f:
        schema = json.dumps(get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, object_id))
        f.write(schema.encode('utf-8') + b'\0')
        with conn.cursor() as cur:
            cur.copy_expert(SQL("COPY {}.{} TO STDOUT WITH (FORMAT 'binary')")
                            .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)), f)


def load_object_from_file(conn, object_id, path):
    logging.info("Loading %s from %s", object_id, path)
    with open(path, 'rb') as f:
        chars = b''
        # Read until the delimiter separating a JSON schema from the Postgres copy_to dump.
        # Surely this is buffered?
        while True:
            c = f.read(1)
            if c == b'\0':
                break
            chars += c

        schema = json.loads(chars.decode('utf-8'))
        create_table(conn, SPLITGRAPH_META_SCHEMA, object_id, schema)

        with conn.cursor() as cur:
            cur.copy_expert(SQL("COPY {}.{} FROM STDIN WITH (FORMAT 'binary')")
                            .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)), f)
