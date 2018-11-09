import gzip

from splitgraph.constants import SPLITGRAPH_META_SCHEMA
from splitgraph.pg_utils import table_dump_generator


def dump_object_to_file(conn, object_id, path):
    with gzip.open(path, 'wb') as f:
        f.writelines(table_dump_generator(conn, SPLITGRAPH_META_SCHEMA, object_id))


def load_object_from_file(conn, path):
    with gzip.open(path, 'rb') as f:
        with conn.cursor() as cur:
            # Insert into the locally checked out schema by default since the dump doesn't have the schema
            # qualification.
            cur.execute("SET search_path TO %s", (SPLITGRAPH_META_SCHEMA,))
            for chunk in f.readlines():
                cur.execute(chunk)
            # Set the schema to (presumably) the default one.
            cur.execute("SET search_path TO public")
