SELECT 
  *
FROM {{ source('some_other_data_raw', 'some_table') }}
