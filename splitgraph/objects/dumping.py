import json

from psycopg2.sql import SQL, Identifier

from splitgraph.connection import get_connection
from splitgraph.constants import SPLITGRAPH_META_SCHEMA
from splitgraph.pg_utils import get_full_table_schema, create_table


# Utilities to dump objects (SNAP/DIFF) into an external format.
# We use a slightly ad hoc format: the schema (JSON) + a null byte + Postgres's copy_to
# binary format (only contains data). There's probably some scope to make this more optimized, maybe
# we should look into columnar on-disk formats (Parquet/Avro) but we currently just want to get the objects
# out of/into postgres as fast as possible.

def dump_object(object_id, fobj):
    """
    Serializes a Splitgraph object into a file or a file-like object.
    :param object_id: Object ID to dump
    :param fobj: File-like object to write the object into
    """
    conn = get_connection()
    schema = json.dumps(get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, object_id))
    fobj.write(schema.encode('utf-8') + b'\0')
    with conn.cursor() as cur:
        cur.copy_expert(SQL("COPY {}.{} TO STDOUT WITH (FORMAT 'binary')")
                        .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)), fobj)


def load_object(object_id, fobj):
    """
    Loads a Splitgraph object from a file / file-like object.
    :param object_id: Object ID to load the object into
    :param fobj: File-like object
    """
    conn = get_connection()
    chars = b''
    # Read until the delimiter separating a JSON schema from the Postgres copy_to dump.
    # Surely this is buffered?
    while True:
        c = fobj.read(1)
        if c == b'\0':
            break
        chars += c

    schema = json.loads(chars.decode('utf-8'))
    create_table(conn, SPLITGRAPH_META_SCHEMA, object_id, schema)

    with conn.cursor() as cur:
        cur.copy_expert(SQL("COPY {}.{} FROM STDIN WITH (FORMAT 'binary')")
                        .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)), fobj)
