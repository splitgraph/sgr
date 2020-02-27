-- API functions to access various parts of splitgraph_meta related to downloading and uploading images.
-- Serves as a level of indirection between Splitgraph push/pull logic and the organisation of the actual
-- SQL tables.
DROP SCHEMA IF EXISTS splitgraph_api CASCADE;

CREATE SCHEMA splitgraph_api;

---
-- Privilege checking
---
-- splitgraph_api.get_current_username()
-- Return the session_user of the current connection (default get_current_username() implementation)
CREATE OR REPLACE FUNCTION splitgraph_api.get_current_username ()
    RETURNS text
    AS $$
BEGIN
    RETURN SESSION_USER;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;

CREATE OR REPLACE FUNCTION splitgraph_api.bypass_privilege ()
    RETURNS BOOLEAN
    AS $$
BEGIN
    -- Superusers bypass everything
    RETURN (
        SELECT usesuper
        FROM pg_user
        WHERE usename = SESSION_USER);
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;

CREATE OR REPLACE FUNCTION splitgraph_api.check_privilege (
    namespace varchar
)
    RETURNS void
    AS $$
BEGIN
    IF splitgraph_api.bypass_privilege () THEN
        RETURN;
    END IF;
    -- Here "current_user" is the definer, "session_user" is the caller and we can access another session variable
    -- to establish identity.
    -- Use IS DISTINCT FROM rather than != to catch namespace=NULL
    IF splitgraph_api.get_current_username () IS DISTINCT FROM namespace THEN
        RAISE insufficient_privilege
        USING MESSAGE = 'You do not have access to this namespace!';
    END IF;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;

---
-- IMAGE API
---
-- get_images(namespace, repository): get metadata for all images in the repository
CREATE OR REPLACE FUNCTION splitgraph_api.get_images (
    _namespace varchar,
    _repository varchar
)
    RETURNS TABLE (
            image_hash varchar,
            parent_id varchar,
            created timestamp,
            comment varchar,
            provenance_type varchar,
            provenance_data jsonb
        )
        AS $$
BEGIN
    RETURN QUERY
    SELECT i.image_hash,
        i.parent_id,
        i.created,
        i.comment,
        i.provenance_type,
        i.provenance_data
    FROM splitgraph_meta.images i
    WHERE i.namespace = _namespace
        AND i.repository = _repository
    ORDER BY created ASC;
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- get_tagged_images(namespace, repository): get hashes of all images with a tag.
CREATE OR REPLACE FUNCTION splitgraph_api.get_tagged_images (
    _namespace varchar,
    _repository varchar
)
    RETURNS TABLE (
            image_hash varchar,
            tag varchar
        )
        AS $$
BEGIN
    RETURN QUERY
    SELECT t.image_hash,
        t.tag
    FROM splitgraph_meta.tags t
    WHERE t.namespace = _namespace
        AND t.repository = _repository;
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- get_image_size(namespace, repository, image_hash): get image size in bytes (counting tables
-- that share objects only once)
CREATE OR REPLACE FUNCTION splitgraph_api.get_image_size (
    _namespace varchar,
    _repository varchar,
    _image_hash varchar
)
    RETURNS INTEGER
    AS $$
BEGIN
    RETURN (WITH iob AS (
            SELECT DISTINCT image_hash,
                unnest(object_ids) AS object_id
            FROM splitgraph_meta.tables t
            WHERE t.namespace = _namespace
                AND t.repository = _repository
                AND t.image_hash = _image_hash
)
        SELECT sum(o.size)
        FROM iob
            JOIN splitgraph_meta.objects o ON iob.object_id = o.object_id);
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- Consider merging writes to all tables into one big routine (e.g. also include a list of tables here, which
-- will get added to the tables table)
-- add_image(namespace, repository, image_hash, parent_id, created, comment, provenance_type, provenance_data)
CREATE OR REPLACE FUNCTION splitgraph_api.add_image (
    namespace varchar,
    repository varchar,
    image_hash varchar,
    parent_id varchar,
    created timestamp,
    comment varchar,
    provenance_type varchar,
    provenance_data jsonb
)
    RETURNS void
    AS $$
BEGIN
    PERFORM splitgraph_api.check_privilege (namespace);
    INSERT INTO splitgraph_meta.images (namespace, repository, image_hash,
	parent_id, created, comment, provenance_type, provenance_data)
	VALUES (namespace, repository, image_hash, parent_id, created, comment,
	    provenance_type, provenance_data);
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- tag_image (namespace, repository, image_hash, tag)
CREATE OR REPLACE FUNCTION splitgraph_api.tag_image (
    _namespace varchar,
    _repository varchar,
    _image_hash varchar,
    _tag varchar
)
    RETURNS void
    AS $$
BEGIN
    PERFORM splitgraph_api.check_privilege (_namespace);
    INSERT INTO splitgraph_meta.tags (namespace, repository, image_hash, tag)
        VALUES (_namespace, _repository, _image_hash, _tag)
    ON CONFLICT (namespace, repository, tag)
        DO UPDATE SET
            image_hash = EXCLUDED.image_hash;
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- delete_repository(namespace, repository)
CREATE OR REPLACE FUNCTION splitgraph_api.delete_repository (
    _namespace varchar,
    _repository varchar
)
    RETURNS void
    AS $$
BEGIN
    PERFORM splitgraph_api.check_privilege (_namespace);
    DELETE FROM splitgraph_meta.tables
    WHERE namespace = _namespace
        AND repository = _repository;
    DELETE FROM splitgraph_meta.tags
    WHERE namespace = _namespace
        AND repository = _repository;
    DELETE FROM splitgraph_meta.images
    WHERE namespace = _namespace
        AND repository = _repository;
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

--
-- OBJECT API
--
-- get_new_objects(object_ids): return objects in object_ids that don't exist in the object tree.
CREATE OR REPLACE FUNCTION splitgraph_api.get_new_objects (
    object_ids varchar[]
)
    RETURNS varchar[]
    AS $$
BEGIN
    RETURN ARRAY (
        SELECT o
        FROM unnest(object_ids) o
        WHERE o NOT IN (
                SELECT object_id
                FROM splitgraph_meta.objects));
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- get_object_meta(object_ids): get metadata for objects
CREATE OR REPLACE FUNCTION splitgraph_api.get_object_meta (
    object_ids varchar[]
)
    RETURNS TABLE (
            object_id varchar,
            format varchar,
            namespace varchar,
            size bigint,
            created timestamp,
            insertion_hash varchar(64),
            deletion_hash varchar(64),
            INDEX JSONB
        )
        AS $$
BEGIN
    RETURN QUERY
    SELECT o.object_id,
        o.format,
        o.namespace,
        o.size,
        o.created,
        o.insertion_hash,
        o.deletion_hash,
        o.index
    FROM splitgraph_meta.objects o
    WHERE o.object_id = ANY (object_ids);
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- get_object_locations(object_ids): get external locations for objects
CREATE OR REPLACE FUNCTION splitgraph_api.get_object_locations (
    object_ids varchar[]
)
    RETURNS TABLE (
            object_id varchar,
            LOCATION VARCHAR,
            protocol varchar
        )
        AS $$
BEGIN
    RETURN QUERY
    SELECT o.object_id,
        o.location,
        o.protocol
    FROM splitgraph_meta.object_locations o
    WHERE o.object_id = ANY (object_ids);
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- add_object(object_id, format, namespace, size, insertion_hash, deletion_hash, index)
-- If the object already exists, it gets overwritten (making sure the caller has permissions
-- to overwrite it) -- this is for easier patching or adding indexes by users.
CREATE OR REPLACE FUNCTION splitgraph_api.add_object (
    _object_id varchar,
    _format varchar,
    _namespace varchar,
    _size bigint,
    _created timestamp,
    _insertion_hash varchar(64),
    _deletion_hash varchar(64),
    _index jsonb
)
    RETURNS void
    AS $$
DECLARE
    existing record;
BEGIN
    PERFORM splitgraph_api.check_privilege (_namespace);
    -- Do SELECT FOR UPDATE to lock the row if it actually does exist to avoid two people
    -- calling this at the same time
    SELECT * INTO existing
    FROM splitgraph_meta.objects
    WHERE splitgraph_meta.objects.object_id = _object_id
    FOR UPDATE;
    IF NOT FOUND THEN
	INSERT INTO splitgraph_meta.objects (object_id, format, namespace,
	    size, created, insertion_hash, deletion_hash, INDEX)
		VALUES (_object_id, _format, _namespace, _size, _created,
		    _insertion_hash, _deletion_hash, _index);
    ELSE
        PERFORM splitgraph_api.check_privilege (existing.namespace);
        UPDATE
            splitgraph_meta.objects
        SET format = _format,
            namespace = _namespace,
            size = _size,
            created = _created,
            insertion_hash = _insertion_hash,
            deletion_hash = _deletion_hash,
            INDEX = _index
        WHERE object_id = _object_id;
    END IF;
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- add_object_location(object_id, location, protocol)
CREATE OR REPLACE FUNCTION splitgraph_api.add_object_location (
    _object_id varchar,
    LOCATION varchar,
    protocol varchar
)
    RETURNS void
    AS $$
DECLARE
    namespace varchar;
BEGIN
    namespace = (
        SELECT o.namespace
        FROM splitgraph_meta.objects o
        WHERE o.object_id = _object_id);
    PERFORM splitgraph_api.check_privilege (namespace);
    INSERT INTO splitgraph_meta.object_locations (object_id, LOCATION, protocol)
        VALUES (_object_id, LOCATION, protocol)
    ON CONFLICT (object_id)
        DO UPDATE SET LOCATION = EXCLUDED.location, protocol = EXCLUDED.protocol;
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

--
-- TABLE API
--
-- get_tables(namespace, repository, image_hash): list all tables in a given image, their schemas and the fragments
-- they consist of.
CREATE OR REPLACE FUNCTION splitgraph_api.get_tables (
    _namespace varchar,
    _repository varchar,
    _image_hash varchar
)
    RETURNS TABLE (
            table_name varchar,
            table_schema jsonb,
            object_ids varchar[]
        )
        AS $$
BEGIN
    RETURN QUERY
    SELECT t.table_name,
        t.table_schema,
        t.object_ids
    FROM splitgraph_meta.tables t
    WHERE t.namespace = _namespace
        AND t.repository = _repository
        AND t.image_hash = _image_hash;
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- add_table(namespace, repository, table_name, table_schema, object_ids) -- add a table to an existing image.
-- Technically, we shouldn't allow this to be done once the image has been created (so maybe that idea with only having
-- two API calls: once to register the objects and one to register the images+tables might work?)
CREATE OR REPLACE FUNCTION splitgraph_api.add_table (
    namespace varchar,
    repository varchar,
    image_hash varchar,
    table_name varchar,
    table_schema jsonb,
    object_ids varchar[]
)
    RETURNS void
    AS $$
BEGIN
    PERFORM splitgraph_api.check_privilege (namespace);
    INSERT INTO splitgraph_meta.tables (namespace, repository, image_hash,
	table_name, table_schema, object_ids)
        VALUES (namespace, repository, image_hash, table_name, table_schema, object_ids);
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- get_table_size(namespace, repository, image_hash, table_name): get table size in bytes (counting tables
-- that share objects only once)
CREATE OR REPLACE FUNCTION splitgraph_api.get_table_size (
    _namespace varchar,
    _repository varchar,
    _image_hash varchar,
    _table_name varchar
)
    RETURNS INTEGER
    AS $$
BEGIN
    RETURN (WITH iob AS (
            SELECT DISTINCT image_hash,
                unnest(object_ids) AS object_id
            FROM splitgraph_meta.tables t
            WHERE t.namespace = _namespace
                AND t.repository = _repository
                AND t.image_hash = _image_hash
                AND t.table_name = _table_name
)
        SELECT sum(o.size)
        FROM iob
            JOIN splitgraph_meta.objects o ON iob.object_id = o.object_id);
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

--
-- PUBLISH API
--
-- publish_image(namespace, repository, tag, image_hash, published, provenance, readme, schemata, previews)
CREATE OR REPLACE FUNCTION splitgraph_api.publish_image (
    namespace varchar,
    repository varchar,
    tag varchar,
    image_hash varchar,
    published timestamp,
    provenance json,
    readme varchar,
    schemata json,
    previews json
)
    RETURNS void
    AS $$
BEGIN
    PERFORM splitgraph_api.check_privilege (namespace);
    INSERT INTO registry_meta.images (namespace, repository, tag, image_hash,
	published, provenance, readme, schemata, previews)
	VALUES (namespace, repository, tag, image_hash, published, provenance,
	    readme, schemata, previews);
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = registry_meta, pg_temp;

CREATE OR REPLACE FUNCTION splitgraph_api.get_published_image (
    _namespace varchar,
    _repository varchar,
    _tag varchar
)
    RETURNS TABLE (
            image_hash varchar,
            published timestamp,
            provenance json,
            readme varchar,
            schemata json,
            previews json
        )
        AS $$
BEGIN
    RETURN QUERY
    SELECT i.image_hash,
        i.published,
        i.provenance,
        i.readme,
        i.schemata,
        i.previews
    FROM registry_meta.images i
    WHERE i.namespace = _namespace
        AND i.repository = _repository
        AND i.tag = _tag;
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = registry_meta, pg_temp;

CREATE OR REPLACE FUNCTION splitgraph_api.unpublish_repository (
    _namespace varchar,
    _repository varchar
)
    RETURNS void
    AS $$
BEGIN
    PERFORM splitgraph_api.check_privilege (_namespace);
    DELETE FROM registry_meta.images
    WHERE namespace = _namespace
        AND repository = _repository;
END
$$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = registry_meta, pg_temp;

--
-- S3 UPLOAD/DOWNLOAD API
--
CREATE EXTENSION IF NOT EXISTS plpython3u;

-- get_object_upload_url(object_id) -> pre-signed URLs to upload the object, its footer and schema
-- TODO we first need to check privilege (for object visibility) and _then_ call into the plpython.
-- Or maybe there's a way to call back into postgres to check privilege.
-- Importing splitgraph.config isn't much slower than importing that + minio
-- (300ms vs 500ms) -- basically no matter what, we can't do it for every object.
CREATE OR REPLACE FUNCTION splitgraph_api.get_object_upload_urls (
    s3_host varchar,
    object_ids varchar[]
)
    RETURNS varchar[][]
    AS $$
    from splitgraph.hooks.s3_server import get_object_upload_urls
    return get_object_upload_urls(s3_host, object_ids)
$$
LANGUAGE plpython3u
SECURITY DEFINER;

-- get_object_download_url(object_id)
CREATE OR REPLACE FUNCTION splitgraph_api.get_object_download_urls (
    s3_host varchar,
    object_ids varchar[]
)
    RETURNS varchar[][]
    AS $$
    from splitgraph.hooks.s3_server import get_object_download_urls
    return get_object_download_urls(s3_host, object_ids)
$$
LANGUAGE plpython3u
SECURITY DEFINER;
