-- Engine-side functions for managing CStore files
-- These import splitgraph.config but since that doesn't pull
-- the rest of splitgraph, the overhead is pretty small.

CREATE EXTENSION IF NOT EXISTS plpython3u;
CREATE SCHEMA IF NOT EXISTS splitgraph_api;

CREATE OR REPLACE FUNCTION splitgraph_api.upload_object(object_id varchar, urls varchar[]) RETURNS void AS
$BODY$
    import os.path
    import requests
    from splitgraph.config import CONFIG

    SG_ENGINE_OBJECT_PATH = CONFIG["SG_ENGINE_OBJECT_PATH"]

    object_path = os.path.join(SG_ENGINE_OBJECT_PATH, object_id)

    for suffix, url in zip(("", ".footer", ".schema"), urls):
        with open(object_path + suffix, "rb") as f:
            response = requests.put(url, data=f)
            response.raise_for_status()
$BODY$
LANGUAGE plpython3u VOLATILE;


CREATE OR REPLACE FUNCTION splitgraph_api.download_object(object_id varchar, urls varchar[]) RETURNS void AS
$BODY$
    import os.path
    import shutil
    import requests
    from splitgraph.config import CONFIG

    SG_ENGINE_OBJECT_PATH = CONFIG["SG_ENGINE_OBJECT_PATH"]

    object_path = os.path.join(SG_ENGINE_OBJECT_PATH, object_id)

    for suffix, url in zip(("", ".footer", ".schema"), urls):
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            with open(object_path + suffix, "wb") as f:
                shutil.copyfileobj(response.raw, f)
$BODY$
LANGUAGE plpython3u VOLATILE;


CREATE OR REPLACE FUNCTION splitgraph_api.set_object_schema(object_id varchar, schema varchar) RETURNS void AS
$BODY$
    import os.path
    from splitgraph.config import CONFIG

    SG_ENGINE_OBJECT_PATH = CONFIG["SG_ENGINE_OBJECT_PATH"]

    with open(os.path.join(SG_ENGINE_OBJECT_PATH, object_id + ".schema"), "w") as f:
        f.write(schema)
$BODY$
LANGUAGE plpython3u VOLATILE;


CREATE OR REPLACE FUNCTION splitgraph_api.get_object_schema(object_id varchar) RETURNS varchar AS
$BODY$
    import os.path
    from splitgraph.config import CONFIG

    SG_ENGINE_OBJECT_PATH = CONFIG["SG_ENGINE_OBJECT_PATH"]

    with open(os.path.join(SG_ENGINE_OBJECT_PATH, object_id + ".schema")) as f:
        return f.read()
$BODY$
LANGUAGE plpython3u VOLATILE;


CREATE OR REPLACE FUNCTION splitgraph_api.delete_object_files(object_id varchar) RETURNS void AS
$BODY$
    import os.path
    from splitgraph.config import CONFIG

    SG_ENGINE_OBJECT_PATH = CONFIG["SG_ENGINE_OBJECT_PATH"]

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


CREATE OR REPLACE FUNCTION splitgraph_api.get_object_size(object_id varchar) RETURNS int AS
$BODY$
    import os.path
    from splitgraph.config import CONFIG

    SG_ENGINE_OBJECT_PATH = CONFIG["SG_ENGINE_OBJECT_PATH"]

    object_path = os.path.join(SG_ENGINE_OBJECT_PATH, object_id)
    return os.path.getsize(object_path) + \
        os.path.getsize(object_path + ".footer") + \
        os.path.getsize(object_path + ".schema")
$BODY$
LANGUAGE plpython3u VOLATILE;


CREATE OR REPLACE FUNCTION splitgraph_api.list_objects() RETURNS varchar[] AS
$BODY$
    import os
    from splitgraph.config import CONFIG

    SG_ENGINE_OBJECT_PATH = CONFIG["SG_ENGINE_OBJECT_PATH"]

    # Crude but faster than listing foreign tables (and hopefully consistent).

    files = os.listdir(SG_ENGINE_OBJECT_PATH)
    return [f for f in files if not f.endswith(".schema") and not f.endswith(".footer")]
$BODY$
LANGUAGE plpython3u VOLATILE;


CREATE OR REPLACE FUNCTION splitgraph_api.object_exists(object_id varchar) RETURNS boolean AS
$BODY$
    # Check if the physical object file exists in storage.

    import os.path
    from splitgraph.config import CONFIG

    SG_ENGINE_OBJECT_PATH = CONFIG["SG_ENGINE_OBJECT_PATH"]

    return os.path.exists(os.path.join(SG_ENGINE_OBJECT_PATH, object_id))
$BODY$
LANGUAGE plpython3u VOLATILE;