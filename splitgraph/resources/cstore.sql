CREATE OR REPLACE FUNCTION splitgraph_upload_object(object_id varchar, endpoint varchar, access_key varchar, secret_key varchar) RETURNS varchar AS
$BODY$
    import os.path
    from minio import Minio
    from minio.error import BucketAlreadyOwnedByYou, BucketAlreadyExists, MinioError

    # tmp, don't want to import splitgraph
    SG_ENGINE_OBJECT_PATH = "/var/lib/splitgraph/objects"

    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )
    bucket = access_key

    try:
        client.make_bucket(bucket)
    except BucketAlreadyOwnedByYou:
        pass
    except BucketAlreadyExists:
        pass

    object_path = os.path.join(SG_ENGINE_OBJECT_PATH, object_id)

    client.fput_object(bucket, object_id, object_path)
    client.fput_object(bucket, object_id + ".footer", object_path + ".footer")
    client.fput_object(bucket, object_id + ".schema", object_path + ".schema")

    return "%s/%s/%s" % (endpoint, bucket, object_id)
$BODY$
LANGUAGE plpython3u VOLATILE;


CREATE OR REPLACE FUNCTION splitgraph_download_object(object_id varchar, url varchar, access_key varchar, secret_key varchar) RETURNS void AS
$BODY$
    import os.path
    from minio import Minio

    SG_ENGINE_OBJECT_PATH = "/var/lib/splitgraph/objects"

    endpoint, bucket, remote_object = url.split("/")
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )
    object_path = os.path.join(SG_ENGINE_OBJECT_PATH, object_id)

    client.fget_object(bucket, remote_object, object_path)
    client.fget_object(bucket, remote_object + ".footer", object_path + ".footer")
    client.fget_object(bucket, remote_object + ".schema", object_path + ".schema")
$BODY$
LANGUAGE plpython3u VOLATILE;


CREATE OR REPLACE FUNCTION splitgraph_set_object_schema(object_id varchar, schema varchar) RETURNS void AS
$BODY$
    import os.path

    SG_ENGINE_OBJECT_PATH = "/var/lib/splitgraph/objects"

    with open(os.path.join(SG_ENGINE_OBJECT_PATH, object_id + ".schema"), "w") as f:
        f.write(schema)
$BODY$
LANGUAGE plpython3u VOLATILE;


CREATE OR REPLACE FUNCTION splitgraph_get_object_schema(object_id varchar) RETURNS varchar AS
$BODY$
    import os.path

    SG_ENGINE_OBJECT_PATH = "/var/lib/splitgraph/objects"

    with open(os.path.join(SG_ENGINE_OBJECT_PATH, object_id + ".schema")) as f:
        return f.read()
$BODY$
LANGUAGE plpython3u VOLATILE;


CREATE OR REPLACE FUNCTION splitgraph_delete_object_files(object_id varchar) RETURNS void AS
$BODY$
    import os.path

    SG_ENGINE_OBJECT_PATH = "/var/lib/splitgraph/objects"

    def _remove(path):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    object_path = os.path.join(SG_ENGINE_OBJECT_PATH, object_id)
    _remove(object_path)
    _remove(object_path + ".footer")
    _remove(object_path + ".schema")
$BODY$
LANGUAGE plpython3u VOLATILE;


CREATE OR REPLACE FUNCTION splitgraph_get_object_size(object_id varchar) RETURNS int AS
$BODY$
    import os.path

    SG_ENGINE_OBJECT_PATH = "/var/lib/splitgraph/objects"

    object_path = os.path.join(SG_ENGINE_OBJECT_PATH, object_id)
    return os.path.getsize(object_path) + \
        os.path.getsize(object_path + ".footer") + \
        os.path.getsize(object_path + ".schema")
$BODY$
LANGUAGE plpython3u VOLATILE;
