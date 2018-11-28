-- external SQL file that gets called from within an splitfile with the default schema set to the mountpoint
-- being produced. Uses SQL-style comments since this whole file gets sent to the driver and doesn't need things
-- like line escapes etc.
CREATE TABLE join_table AS SELECT fruit_id AS id, my_fruits.name AS fruit, vegetables.name AS vegetable
                                FROM my_fruits JOIN vegetables
                                ON fruit_id = vegetable_id