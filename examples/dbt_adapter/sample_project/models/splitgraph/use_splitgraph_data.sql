{{ config(materialized='table') }}

with source_data as (

    select domain, count(domain) as count
    from "splitgraph/socrata:latest".datasets
    group by domain

)

select *
from source_data
