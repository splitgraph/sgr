-- API functions to access various parts of splitgraph_meta related to downloading and uploading images.
-- Serves as a level of indirection between Splitgraph push/pull logic and the organisation of the actual
-- SQL tables.

DROP SCHEMA IF EXISTS splitgraph_api CASCADE;
CREATE SCHEMA splitgraph_api;

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
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION splitgraph_api.get_object_path(object_ids varchar[]) RETURNS varchar[] AS $$
BEGIN
    RETURN ARRAY(WITH RECURSIVE parents AS
        (SELECT object_id, parent_id FROM splitgraph_meta.objects WHERE object_id = ANY(object_ids)
            UNION ALL SELECT o.object_id, o.parent_id
            FROM parents p JOIN splitgraph_meta.objects o ON p.parent_id = o.object_id)
        SELECT object_id FROM parents);
END
$$ LANGUAGE plpgsql SECURITY DEFINER;