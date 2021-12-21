SELECT 
  *
FROM {{ source('and_third_data', 'some_table') }}
