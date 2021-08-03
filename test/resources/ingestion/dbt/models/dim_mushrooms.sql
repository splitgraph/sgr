SELECT
    (_airbyte_data ->> 'name')::text AS mushroom_name,
    (_airbyte_data ->> 'discovery')::timestamp AS discovered_on
FROM {{ source('airbyte_raw', '_airbyte_raw_mushrooms') }}