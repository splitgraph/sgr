CREATE INDEX idx_table_objects ON splitgraph_meta.tables USING GIN(object_ids);
