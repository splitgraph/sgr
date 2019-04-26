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
CREATE OR REPLACE FUNCTION splitgraph_api.get_current_username()
RETURNS text AS $$
BEGIN
    RETURN session_user;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


CREATE OR REPLACE FUNCTION splitgraph_api.check_privilege(namespace varchar) RETURNS void AS $$
BEGIN
    IF (SELECT usesuper FROM pg_user WHERE usename = session_user) THEN
        -- Superusers bypass everything
        RETURN;
    END IF;
    -- Here "current_user" is the definer, "session_user" is the caller and we can access another session variable
    -- to establish identity.
    -- Use IS DISTINCT FROM rather than != to catch namespace=NULL
    IF splitgraph_api.get_current_username() IS DISTINCT FROM namespace THEN
        RAISE insufficient_privilege USING MESSAGE = 'You do not have access to this namespace!';
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


---
-- IMAGE API
---

-- get_images(namespace, repository): get metadata for all images in the repository
CREATE OR REPLACE FUNCTION splitgraph_api.get_images(_namespace varchar, _repository varchar)
  RETURNS TABLE (
    image_hash      VARCHAR,
    parent_id       VARCHAR,
    created         TIMESTAMP,
    comment         VARCHAR,
    provenance_type VARCHAR,
    provenance_data JSONB) AS $$
BEGIN
   RETURN QUERY
   SELECT i.image_hash, i.parent_id, i.created, i.comment, i.provenance_type, i.provenance_data
   FROM splitgraph_meta.images i
   WHERE i.namespace = _namespace and i.repository = _repository
   ORDER BY created ASC;
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- get_tagged_images(namespace, repository): get hashes of all images with a tag.
CREATE OR REPLACE FUNCTION splitgraph_api.get_tagged_images(_namespace varchar, _repository varchar)
  RETURNS TABLE (
    image_hash VARCHAR,
    tag        VARCHAR) AS $$
BEGIN
   RETURN QUERY
   SELECT t.image_hash, t.tag
   FROM splitgraph_meta.tags t
   WHERE t.namespace = _namespace and t.repository = _repository;
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- Consider merging writes to all tables into one big routine (e.g. also include a list of tables here, which
-- will get added to the tables table)
-- add_image(namespace, repository, image_hash, parent_id, created, comment, provenance_type, provenance_data)
CREATE OR REPLACE FUNCTION splitgraph_api.add_image(
    namespace varchar, repository varchar, image_hash varchar, parent_id varchar, created timestamp, comment varchar,
    provenance_type varchar, provenance_data jsonb) RETURNS void AS $$
BEGIN
    PERFORM splitgraph_api.check_privilege(namespace);
    INSERT INTO splitgraph_meta.images(namespace, repository, image_hash, parent_id, created, comment,
        provenance_type, provenance_data)
    VALUES (namespace, repository, image_hash, parent_id, created, comment, provenance_type, provenance_data);
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- tag_image (namespace, repository, image_hash, tag)
CREATE OR REPLACE FUNCTION splitgraph_api.tag_image(
    _namespace varchar, _repository varchar, _image_hash varchar, _tag varchar) RETURNS void AS $$
BEGIN
    PERFORM splitgraph_api.check_privilege(_namespace);
    INSERT INTO splitgraph_meta.tags(namespace, repository, image_hash, tag)
    VALUES (_namespace, _repository, _image_hash, _tag)
    ON CONFLICT (namespace, repository, tag) DO UPDATE SET image_hash = EXCLUDED.image_hash;
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- delete_repository(namespace, repository)
CREATE OR REPLACE FUNCTION splitgraph_api.delete_repository(_namespace varchar, _repository varchar) RETURNS void AS $$
BEGIN
    PERFORM splitgraph_api.check_privilege(_namespace);
    DELETE FROM splitgraph_meta.tables WHERE namespace = _namespace AND repository = _repository;
    DELETE FROM splitgraph_meta.tags WHERE namespace = _namespace AND repository = _repository;
    DELETE FROM splitgraph_meta.images WHERE namespace = _namespace AND repository = _repository;
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;


--
-- OBJECT API
--

-- get_object_path(object_ids): list all objects that object_ids depend on, recursively
CREATE OR REPLACE FUNCTION splitgraph_api.get_object_path(object_ids varchar[]) RETURNS varchar[] AS $$
DECLARE result varchar[];
BEGIN
    -- Something weird happens if this function is invoked exactly five times and the execution
    -- time goes from ~6ms to ~100ms. Possibly something to do with the query planner?
    EXECUTE 'SELECT ARRAY(WITH RECURSIVE parents AS'
        ' (SELECT object_id, parent_id FROM splitgraph_meta.objects WHERE object_id = ANY($1)'
        '    UNION ALL SELECT o.object_id, o.parent_id'
        '    FROM parents p JOIN splitgraph_meta.objects o ON p.parent_id = o.object_id)'
        ' SELECT object_id FROM parents)' USING object_ids INTO result;
    RETURN result;
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- get_new_objects(object_ids): return objects in object_ids that don't exist in the object tree.
CREATE OR REPLACE FUNCTION splitgraph_api.get_new_objects(object_ids varchar[]) RETURNS varchar[] AS $$
BEGIN
    RETURN ARRAY(SELECT o
        FROM unnest(object_ids) o
        WHERE o NOT IN (SELECT object_id FROM splitgraph_meta.objects));
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- get_object_meta(object_ids): get metadata for objects
CREATE OR REPLACE FUNCTION splitgraph_api.get_object_meta(object_ids varchar[])
  RETURNS TABLE (
    object_id      VARCHAR,
    format         VARCHAR,
    parent_id      VARCHAR,
    namespace      VARCHAR,
    size           BIGINT,
    insertion_hash VARCHAR(64),
    deletion_hash  VARCHAR(64),
    index          JSONB) AS $$
BEGIN
   RETURN QUERY
   SELECT o.object_id, o.format, o.parent_id, o.namespace, o.size, o.insertion_hash, o.deletion_hash, o.index
   FROM splitgraph_meta.objects o
   WHERE o.object_id = ANY(object_ids);
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- get_object_locations(object_ids): get external locations for objects
CREATE OR REPLACE FUNCTION splitgraph_api.get_object_locations(object_ids varchar[])
  RETURNS TABLE (
    object_id VARCHAR,
    location  VARCHAR,
    protocol  VARCHAR) AS $$
BEGIN
   RETURN QUERY
   SELECT o.object_id, o.location, o.protocol
   FROM splitgraph_meta.object_locations o
   WHERE o.object_id = ANY(object_ids);
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- add_object(object_id, format, parent_id, namespace, size, insertion_hash, deletion_hash, index)
CREATE OR REPLACE FUNCTION splitgraph_api.add_object(object_id varchar, format varchar, parent_id varchar,
    namespace varchar, size bigint, insertion_hash varchar(64),
    deletion_hash varchar(64), index jsonb) RETURNS void AS $$
BEGIN
    PERFORM splitgraph_api.check_privilege(namespace);
    INSERT INTO splitgraph_meta.objects(object_id, format, parent_id, namespace, size,
        insertion_hash, deletion_hash, index)
    VALUES (object_id, format, parent_id, namespace, size, insertion_hash, deletion_hash, index);
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- add_object_location(object_id, location, protocol)
CREATE OR REPLACE FUNCTION splitgraph_api.add_object_location(_object_id varchar, location varchar, protocol varchar)
    RETURNS void AS $$
DECLARE namespace VARCHAR;
BEGIN
    namespace = (SELECT o.namespace FROM splitgraph_meta.objects o WHERE o.object_id = _object_id);
    PERFORM splitgraph_api.check_privilege(namespace);
    INSERT INTO splitgraph_meta.object_locations(object_id, location, protocol)
    VALUES (_object_id, location, protocol);
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;


--
-- TABLE API
--

-- get_tables(namespace, repository, image_hash): list all tables in a given image, their schemas and the fragments
-- they consist of.
CREATE OR REPLACE FUNCTION splitgraph_api.get_tables(_namespace varchar, _repository varchar, _image_hash varchar)
  RETURNS TABLE (
    table_name VARCHAR,
    table_schema JSONB,
    object_ids VARCHAR[]) AS $$
BEGIN
  RETURN QUERY
  SELECT t.table_name, t.table_schema, t.object_ids
  FROM splitgraph_meta.tables t
  WHERE t.namespace = _namespace AND t.repository = _repository AND t.image_hash = _image_hash;
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

-- add_table(namespace, repository, table_name, table_schema, object_ids) -- add a table to an existing image.
-- Technically, we shouldn't allow this to be done once the image has been created (so maybe that idea with only having
-- two API calls: once to register the objects and one to register the images+tables might work?)
CREATE OR REPLACE FUNCTION splitgraph_api.add_table(
    namespace varchar, repository varchar, image_hash varchar, table_name varchar,
    table_schema jsonb, object_ids varchar[]) RETURNS void AS $$
BEGIN
    PERFORM splitgraph_api.check_privilege(namespace);
    INSERT INTO splitgraph_meta.tables(namespace, repository, image_hash, table_name, table_schema, object_ids)
    VALUES (namespace, repository, image_hash, table_name, table_schema, object_ids);
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = splitgraph_meta, pg_temp;

--
-- PUBLISH API
--

-- publish_image(namespace, repository, tag, image_hash, published, provenance, readme, schemata, previews)
CREATE OR REPLACE FUNCTION splitgraph_api.publish_image(
    namespace varchar, repository varchar, tag varchar, image_hash varchar, published timestamp, provenance json,
    readme varchar, schemata json, previews json) RETURNS void AS $$
BEGIN
    PERFORM splitgraph_api.check_privilege(namespace);
    INSERT INTO registry_meta.images(namespace, repository, tag, image_hash, published, provenance, readme, schemata,
        previews)
    VALUES (namespace, repository, tag, image_hash, published, provenance, readme, schemata, previews);
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = registry_meta, pg_temp;

CREATE OR REPLACE FUNCTION splitgraph_api.get_published_image(_namespace varchar, _repository varchar, _tag varchar)
  RETURNS TABLE (
    image_hash VARCHAR,
    published  TIMESTAMP,
    provenance JSON,
    readme     VARCHAR,
    schemata   JSON,
    previews   JSON) AS $$
BEGIN
   RETURN QUERY
   SELECT i.image_hash, i.published, i.provenance, i.readme, i.schemata, i.previews
   FROM registry_meta.images i
   WHERE i.namespace = _namespace AND i.repository = _repository AND i.tag = _tag;
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = registry_meta, pg_temp;

CREATE OR REPLACE FUNCTION splitgraph_api.unpublish_repository(_namespace varchar, _repository varchar)
RETURNS void AS $$ BEGIN
    PERFORM splitgraph_api.check_privilege(_namespace);
    DELETE FROM registry_meta.images WHERE namespace = _namespace AND repository = _repository;
END
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = registry_meta, pg_temp;
