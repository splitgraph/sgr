import json
import tempfile
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from minio import Minio
from minio.error import (BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)
from psycopg2.sql import SQL, Identifier

from splitgraph import CONFIG
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.connection import get_connection
from splitgraph.pg_utils import get_full_table_schema, create_table

S3_HOST = CONFIG["SG_S3_HOST"]
S3_PORT = CONFIG["SG_S3_PORT"]
S3_ACCESS_KEY = CONFIG["SG_S3_KEY"]
S3_SECRET_KEY = CONFIG["SG_S3_PWD"]


def _ensure_bucket(client, bucket):
    # Make a bucket with the make_bucket API call.
    try:
        client.make_bucket(bucket)
    except BucketAlreadyOwnedByYou:
        pass
    except BucketAlreadyExists:
        pass


def s3_upload_objects(objects_to_push, params):
    """Uploads the objects to S3/S3-compatible host using the Minio client.

    :param objects_to_push: List of object IDs to upload
    :param params: A dictionary of parameters overriding the .sgconfig:

        * host: default SG_S3_HOST
        * port: default SG_S3_PORT
        * access_key: default SG_S3_KEY
        * bucket: default same as access_key
        * secret_key: default SG_S3_PWD

        You can also specify the number of worker threads (`threads`) used to upload the
        objects.
    """
    access_key = params.get('access_key', S3_ACCESS_KEY)
    endpoint = '%s:%s' % (params.get('host', S3_HOST), params.get('port', S3_PORT))
    bucket = params.get('bucket', access_key)
    worker_threads = params.get('threads', 4)
    client = Minio(endpoint,
                   access_key=access_key,
                   secret_key=params.get('secret_key', S3_SECRET_KEY),
                   secure=False)
    _ensure_bucket(client, bucket)

    # Psycopg connection objects are not threadsafe -- make sure two threads can't use it at the same time.
    # In the future, we should replace this with a pg connection pool instead
    pg_conn_lock = Lock()

    with tempfile.TemporaryDirectory() as tmpdir:
        def _do_upload(object_id):
            # First cut: dump the object to file and then upload it using Minio
            tmp_path = tmpdir + '/' + object_id

            with pg_conn_lock:
                with open(tmp_path, 'wb') as f:
                    dump_object(object_id, f)
            # Minio pushes can happen concurrently
            client.fput_object(bucket, object_id, tmp_path)

            return '%s/%s/%s' % (endpoint, bucket, object_id)

        with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
            urls = tpe.map(_do_upload, objects_to_push)

    return urls


def s3_download_objects(objects_to_fetch, params):
    # Maybe here we have to set these to None (anonymous) if the S3 host name doesn't match our own one.
    access_key = params.get('access_key', S3_ACCESS_KEY)
    secret_key = params.get('secret_key', S3_SECRET_KEY)
    worker_threads = params.get('threads', 4)

    pg_conn_lock = Lock()

    with tempfile.TemporaryDirectory() as tmpdir:
        def _do_download(obj_id_url):
            object_id, object_url = obj_id_url
            endpoint, bucket, remote_object = object_url.split('/')
            client = Minio(endpoint,
                           access_key=access_key,
                           secret_key=secret_key,
                           secure=False)

            local_path = tmpdir + '/' + remote_object
            client.fget_object(bucket, remote_object, local_path)

            with pg_conn_lock:
                with open(local_path, 'rb') as f:
                    load_object(object_id, f)

        # Again, first download the objects and then import them (maybe can do streaming later)
        with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
            tpe.map(_do_download, objects_to_fetch)


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
