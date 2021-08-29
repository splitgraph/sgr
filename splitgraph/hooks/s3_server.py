"""S3 registry-side routines called from the Python stored procedure
that are aware of the actual S3 access creds and generate pre-signed
URLs to upload/download objects."""
from datetime import timedelta
from typing import List

from minio.api import Minio
from minio.deleteobjects import DeleteObject
from splitgraph.config import CONFIG

S3_HOST = CONFIG["SG_S3_HOST"]
S3_PORT = CONFIG["SG_S3_PORT"]
S3_SECRET_KEY = CONFIG["SG_S3_PWD"]
S3_BUCKET = CONFIG["SG_S3_BUCKET"]
S3_ACCESS_KEY = CONFIG["SG_S3_KEY"]
S3_SECURE = CONFIG["SG_S3_SECURE"] == "true"

MINIO = Minio(
    "%s:%s" % (S3_HOST, S3_PORT),
    access_key=S3_ACCESS_KEY,
    secret_key=S3_SECRET_KEY,
    secure=S3_SECURE,
)

_EXP = timedelta(seconds=60)


def get_object_upload_urls(s3_host: str, object_ids: List[str]) -> List[List[str]]:
    """
    Return a list of pre-signed URLs that each part of an object can be downloaded from.

    :param s3_host: S3 host that the objects are stored on
    :param object_ids: List of object IDs
    :return: A list of lists [(object URL, object footer URL, object schema URL)]
    """
    # Currently s3_host is ignored: this is to future-proof for choosing between multiple
    # S3 providers by the client but at this point the registry knows exactly where the
    # object is located.
    return [
        [
            MINIO.presigned_put_object(
                bucket_name=S3_BUCKET, object_name=object_id + suffix, expires=_EXP
            )
            for suffix in ("", ".footer", ".schema")
        ]
        for object_id in object_ids
    ]


def get_object_download_urls(s3_host: str, object_ids: List[str]) -> List[List[str]]:
    """
    Return a list of pre-signed URLs that each part of an object can be downloaded from.

    :param s3_host: S3 host that the objects are stored on
    :param object_ids: List of object IDs
    :return: A list of lists [(object URL, object footer URL, object schema URL)]
    """
    return [
        [
            MINIO.presigned_get_object(
                bucket_name=S3_BUCKET, object_name=object_id + suffix, expires=_EXP
            )
            for suffix in ("", ".footer", ".schema")
        ]
        for object_id in object_ids
    ]


def delete_objects(client: Minio, object_ids: List[str]) -> None:
    """
    Delete objects stored in Minio

    :param client: Minio client
    :param object_ids: List of Splitgraph object IDs to delete
    """

    # Expand the list of objects into actual files we store in Minio
    all_object_ids = [o + suffix for o in object_ids for suffix in ("", ".schema", ".footer")]
    list(client.remove_objects(S3_BUCKET, map(DeleteObject, all_object_ids)))


def list_objects(client: Minio) -> List[str]:
    """
    List objects stored in Minio

    :param client: Minio client
    :return: List of Splitgraph object IDs
    """

    return [
        o.object_name
        for o in client.list_objects(bucket_name=S3_BUCKET)
        if not o.object_name.endswith(".footer") and not o.object_name.endswith(".schema")
    ]
