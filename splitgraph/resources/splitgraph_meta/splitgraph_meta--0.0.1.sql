CREATE SCHEMA splitgraph_meta;

-- maybe FK parent_id on image_hash. NULL there means this is the repo root.
CREATE TABLE splitgraph_meta.images (
    namespace varchar NOT NULL,
    repository varchar NOT NULL,
    image_hash varchar(64) NOT NULL CHECK (image_hash ~ '^[a-f0-9]{64}$'),
    parent_id varchar(64) CHECK (parent_id ~ '^[a-f0-9]{64}$' AND parent_id != image_hash),
    created timestamp,
    comment varchar(4096),
    provenance_data jsonb,
    PRIMARY KEY (namespace, repository, image_hash)
);

-- Create an index on the provenance field to allow reverse lookups.
CREATE INDEX idx_image_provenance ON splitgraph_meta.images USING GIN
    (provenance_data jsonb_path_ops);

CREATE TABLE splitgraph_meta.tags (
    namespace varchar NOT NULL,
    repository varchar NOT NULL,
    image_hash varchar,
    tag varchar(64),
    PRIMARY KEY (namespace, repository, tag),
    CONSTRAINT sh_fk FOREIGN KEY (namespace, repository, image_hash) REFERENCES
	splitgraph_meta.images
);

-- Object metadata.
-- * object_id: ID of the object, calculated as sha256((insertion_hash - deletion_hash) + sha256(table_schema)
--     (-) is the vector hash subtraction operator (see `fragment_manager.Digest`)
--     (+) is normal string concatenation
--     table_schema is str(table_schema) of the table this fragment belongs to (as specified in the `tables` table)
--       that encodes the column order, types, names and whether they're the primary key.
-- * insertion_hash: the homomorphic hash of all rows inserted or upserted by this fragment (sum of sha256 hashes
--     of every row). Can be verified by running `FragmentManager.calculate_fragment_insertion_hash`.
-- * deletion_hash: the homomorphic hash of the old values of all rows that were deleted or updated by this fragment.
--     This can't be verified directly by looking at the fragment (since it only contains the PKs of its deleted
--     rows): fetching all the fragments this fragment depends on is required.
--     insertion_hash - deletion hash form the content hash of this fragment.
--     Homomorphic hashing in this case has the property that the sum of content hashes of individual fragments
--     is equal to the content hash of the final materialized table.
-- * namespace: Original namespace this object was created in. Only the users with write rights to a given namespace
--   can delete/alter this object's metadata.
-- * size: the on-disk (in-database) size occupied by the object table as reported by the engine
--   (not the size stored externally).
-- * format: Format of the object. Currently, only FRAG (splitting the table into multiple chunks that can partially
--     overwrite each other) is supported.
-- * index: A JSON object mapping columns spanned by this object to their minimum and maximum values. Used to
--   discard and not download at all objects that definitely don't match a given query.
CREATE TABLE splitgraph_meta.objects (
    object_id varchar NOT NULL PRIMARY KEY CHECK (object_id ~ '^o[a-f0-9]{62}$'),
    namespace varchar NOT NULL,
    size bigint,
    created timestamp,
    format varchar NOT NULL,
    index JSONB,
    insertion_hash varchar(64) NOT NULL CHECK (insertion_hash ~ '^[a-f0-9]{64}$'),
    deletion_hash varchar(64) NOT NULL CHECK (deletion_hash ~ '^[a-f0-9]{64}$'),
    CONSTRAINT valid_format CHECK (format IN ('FRAG'))
);

-- Keep track of objects that have been cached locally on the engine.
--
-- refcount:  incremented when a component requests the object to be downloaded (for materialization
--            or a layered query). Decremented when the component has finished using the object.
--            (maybe consider a row-level lock on this table?)
-- ready:     f if the object can't be used yet (is being downloaded), t if it can.
-- last_used: Timestamp (UTC) this object was last returned to be used in a layered query / materialization.
CREATE TABLE splitgraph_meta.object_cache_status (
    object_id varchar NOT NULL PRIMARY KEY,
    refcount integer,
    ready boolean,
    last_used timestamp
);

-- Size of all objects cached on the engine (existing externally and downloaded for a materialization/LQ)
CREATE TABLE splitgraph_meta.object_cache_occupancy (
    total_size bigint
);

INSERT INTO splitgraph_meta.object_cache_occupancy
    VALUES (0);

-- Maps a given table at a given point in time to a list of fragments that it's assembled from.
CREATE TABLE splitgraph_meta.tables (
    namespace varchar NOT NULL,
    repository varchar NOT NULL,
    image_hash varchar NOT NULL,
    table_name varchar NOT NULL,
    table_schema jsonb,
    object_ids varchar[] NOT NULL,
    PRIMARY KEY (namespace, repository, image_hash, table_name),
    CONSTRAINT tb_fk FOREIGN KEY (namespace, repository, image_hash) REFERENCES
	splitgraph_meta.images
);

CREATE OR REPLACE FUNCTION splitgraph_meta.validate_table_objects ()
    RETURNS TRIGGER
    AS $$
DECLARE
    missing_objects_count int;
BEGIN
    missing_objects_count = (
        SELECT count(*)
        FROM unnest(NEW.object_ids) AS o (object_id)
        WHERE NOT EXISTS (
                SELECT *
                FROM splitgraph_meta.objects
                WHERE object_id = o.object_id));
    IF (missing_objects_count != 0) THEN
        RAISE check_violation
        USING message = 'Some objects in the object_ids array aren''t registered!';
    END IF;
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER sg_validate_table_objects_trigger
    BEFORE INSERT OR UPDATE ON splitgraph_meta.tables
    FOR EACH ROW
    EXECUTE PROCEDURE splitgraph_meta.validate_table_objects ();

-- Keep track of what the remotes for a given repository are (by default, we create an "origin" remote
-- on initial pull)
CREATE TABLE splitgraph_meta.upstream (
    namespace varchar NOT NULL,
    repository varchar NOT NULL,
    remote_name varchar NOT NULL,
    remote_namespace varchar NOT NULL,
    remote_repository varchar NOT NULL,
    PRIMARY KEY (namespace, repository)
);

-- Map objects to their locations for when they don't live on the remote or the local machine but instead
-- in S3/some FTP/HTTP server/torrent etc.
-- Lookup path to resolve an object on checkout: local -> this table -> remote (so that we don't bombard
-- the remote with queries for tables that may have been uploaded to a different place).
CREATE TABLE splitgraph_meta.object_locations (
    object_id varchar NOT NULL,
    location VARCHAR NOT NULL,
    protocol varchar NOT NULL,
    PRIMARY KEY (object_id),
    CONSTRAINT ol_fk FOREIGN KEY (object_id) REFERENCES splitgraph_meta.objects
);

-- Miscellaneous key-value information for this engine (e.g. whether uploading objects is permitted etc).
CREATE TABLE splitgraph_meta.info (
    key VARCHAR NOT NULL,
    value varchar NOT NULL,
    PRIMARY KEY (KEY)
);
