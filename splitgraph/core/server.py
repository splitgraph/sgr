"""Routines that are run inside of the engine,
here so that they can get type- and syntax-checked.

When inside of an LQFDW shim, these are called directly by the Splitgraph core code
to avoid a redundant connection to the engine.
"""

import os.path
from urllib.parse import urlparse

from splitgraph.config import CONFIG

SG_ENGINE_OBJECT_PATH = CONFIG["SG_ENGINE_OBJECT_PATH"]


def verify(url):
    # If there's a file called /rootCA.pem in the engine, use it as the CA for
    # HTTPS S3 operations with .test domains (for testing with self-signed certs)
    if urlparse(url).hostname.split(".")[-1] == "test":
        return "/rootCA.pem" if os.path.exists("/rootCA.pem") else False
    else:
        return True


def _remove(path):
    # Wrapper around remove that doesn't throw if the file doesn't exist.
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def upload_object(object_id, urls):
    import requests

    object_path = os.path.join(SG_ENGINE_OBJECT_PATH, object_id)

    for suffix, url in zip(("", ".footer", ".schema"), urls):
        with open(object_path + suffix, "rb") as f:
            response = requests.put(url, data=f, verify=verify(url))
            response.raise_for_status()


def download_object(object_id, urls):
    import shutil
    import requests

    object_path = os.path.join(SG_ENGINE_OBJECT_PATH, object_id)
    for suffix, url in zip(("", ".footer", ".schema"), urls):
        with requests.get(url, stream=True, verify=verify(url)) as response:
            response.raise_for_status()
            with open(object_path + suffix, "wb") as f:
                shutil.copyfileobj(response.raw, f)


def set_object_schema(object_id, schema):
    with open(os.path.join(SG_ENGINE_OBJECT_PATH, object_id + ".schema"), "w") as f:
        f.write(schema)


def get_object_schema(object_id):
    with open(os.path.join(SG_ENGINE_OBJECT_PATH, object_id + ".schema")) as f:
        return f.read()


def delete_object_files(object_id):
    object_path = os.path.join(SG_ENGINE_OBJECT_PATH, object_id)
    _remove(object_path)
    _remove(object_path + ".footer")
    _remove(object_path + ".schema")


def get_object_size(object_id):
    object_path = os.path.join(SG_ENGINE_OBJECT_PATH, object_id)
    return (
        os.path.getsize(object_path)
        + os.path.getsize(object_path + ".footer")
        + os.path.getsize(object_path + ".schema")
    )


def list_objects():
    from collections import defaultdict

    # Crude but faster than listing foreign tables (and hopefully consistent).
    files = os.listdir(SG_ENGINE_OBJECT_PATH)

    # Make sure to only return objects that have been fully downloaded.
    objects = defaultdict(list)
    for f in files:
        objects[f.replace(".schema", "").replace(".footer", "")].append(f)

    return [f for f, fs in objects.items() if len(fs) == 3]


def object_exists(object_id):
    # Check if the physical object file exists in storage.
    # Make sure to check for all 3 files to guard against partially failed writes.
    return all(
        os.path.exists(os.path.join(SG_ENGINE_OBJECT_PATH, object_id + suffix))
        for suffix in ("", ".footer", ".schema")
    )
