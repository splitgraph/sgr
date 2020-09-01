-- Engine-side functions for managing CStore files
CREATE EXTENSION IF NOT EXISTS plpython3u;

CREATE SCHEMA IF NOT EXISTS splitgraph_api;

-- This is here because it's only supposed to be installed on engines rather than
-- on the registry.
CREATE OR REPLACE FUNCTION splitgraph_api.get_splitgraph_version ()
    RETURNS TEXT
    AS $$
    from splitgraph.__version__ import __version__
    return __version__
$$
LANGUAGE plpython3u
SECURITY INVOKER;

CREATE OR REPLACE FUNCTION splitgraph_api.upload_object (
    object_id varchar,
    urls varchar[]
)
    RETURNS void
    AS $BODY$
    from splitgraph.core.server import upload_object
    upload_object(object_id, urls)

$BODY$
LANGUAGE plpython3u
VOLATILE;

CREATE OR REPLACE FUNCTION splitgraph_api.download_object (
    object_id varchar,
    urls varchar[]
)
    RETURNS void
    AS $BODY$
    from splitgraph.core.server import download_object
    download_object(object_id, urls)

$BODY$
LANGUAGE plpython3u
VOLATILE;

CREATE OR REPLACE FUNCTION splitgraph_api.set_object_schema (
    object_id varchar,
    SCHEMA varchar
)
    RETURNS void
    AS $BODY$
    from splitgraph.core.server import set_object_schema
    set_object_schema(object_id, schema)
$BODY$
LANGUAGE plpython3u
VOLATILE;

CREATE OR REPLACE FUNCTION splitgraph_api.get_object_schema (
    object_id varchar
)
    RETURNS varchar
    AS $BODY$
    from splitgraph.core.server import get_object_schema
    return get_object_schema(object_id)
$BODY$
LANGUAGE plpython3u
VOLATILE;

CREATE OR REPLACE FUNCTION splitgraph_api.delete_object_files (
    object_id varchar
)
    RETURNS void
    AS $BODY$
    from splitgraph.core.server import delete_object_files
    delete_object_files(object_id)
$BODY$
LANGUAGE plpython3u
VOLATILE;

CREATE OR REPLACE FUNCTION splitgraph_api.rename_object_files (
    old_object_id varchar,
    new_object_id varchar
)
    RETURNS void
    AS $BODY$
    from splitgraph.core.server import rename_object_files
    rename_object_files(old_object_id, new_object_id)
$BODY$
LANGUAGE plpython3u
VOLATILE;

CREATE OR REPLACE FUNCTION splitgraph_api.get_object_size (
    object_id varchar
)
    RETURNS int
    AS $BODY$
    from splitgraph.core.server import get_object_size
    return get_object_size(object_id)
$BODY$
LANGUAGE plpython3u
VOLATILE;

CREATE OR REPLACE FUNCTION splitgraph_api.list_objects ()
    RETURNS varchar[]
    AS $BODY$
    from splitgraph.core.server import list_objects
    return list_objects()
$BODY$
LANGUAGE plpython3u
VOLATILE;

CREATE OR REPLACE FUNCTION splitgraph_api.object_exists (
    object_id varchar
)
    RETURNS boolean
    AS $BODY$
    from splitgraph.core.server import object_exists
    return object_exists(object_id)
$BODY$
LANGUAGE plpython3u
VOLATILE;
