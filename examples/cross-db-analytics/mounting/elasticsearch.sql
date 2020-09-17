DROP SCHEMA IF EXISTS elasticsearch CASCADE;
CREATE SCHEMA elasticsearch;

CREATE VIEW elasticsearch.table_1 AS (
    SELECT
        "@timestamp" AS timestamp,
        col_1,
        col_2
    FROM elasticsearch_raw.table_1
    -- You can use query='lucene query string' to pass queries...
    WHERE query = 'col_3:some_text'
);

CREATE MATERIALIZED VIEW elasticsearch.table_1_filtered AS (
    SELECT
        timestamp,
        col_1,
        col_2
    FROM elasticsearch.table_1
    -- ...or use PG qualifiers (this will push the predicate down to ES)
    WHERE col_1 > 42
);

CREATE VIEW elasticsearch.big_join AS (
    SELECT
        t1.timestamp,
        t1.col_1 AS t1_col_1,
        t1.col_2 AS t2_col_2,
        t2.col_2 AS t2_col_1
    FROM elasticsearch.table_1_filtered t1
    JOIN elasticsearch_raw.table_2 t2
    ON t1.timestamp = t2.timestamp
    ORDER BY timestamp ASC
);
