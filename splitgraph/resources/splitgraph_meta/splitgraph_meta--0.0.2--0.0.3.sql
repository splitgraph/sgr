-- Add restrictions on maximum repository length and names
ALTER TABLE splitgraph_meta.images
    ALTER COLUMN repository SET DATA TYPE varchar(64);
ALTER TABLE splitgraph_meta.images
    ALTER COLUMN namespace SET DATA TYPE varchar(64);
ALTER TABLE splitgraph_meta.images
    ADD CONSTRAINT images_repository CHECK (repository ~ '^[-A-Za-z0-9_]+$');
ALTER TABLE splitgraph_meta.images
    ADD CONSTRAINT images_namespace CHECK (namespace ~ '^[-A-Za-z0-9_]*$');

ALTER TABLE splitgraph_meta.objects
    ALTER COLUMN namespace SET DATA TYPE varchar(64);
ALTER TABLE splitgraph_meta.objects
    ADD CONSTRAINT objects_namespace CHECK (namespace ~ '^[-A-Za-z0-9_]*$');
