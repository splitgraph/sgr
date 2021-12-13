SELECT 
  *
FROM {{ source('some_data_source', 'some_table') }}
