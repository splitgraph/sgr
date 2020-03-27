-- Migration to add the rows added/deleted metadata to every object in object_meta
ALTER TABLE splitgraph_meta.objects
    ADD COLUMN rows_inserted integer DEFAULT 0 CHECK (rows_inserted >= 0);

ALTER TABLE splitgraph_meta.objects
    ADD COLUMN rows_deleted integer DEFAULT 0 CHECK (rows_deleted >= 0);
