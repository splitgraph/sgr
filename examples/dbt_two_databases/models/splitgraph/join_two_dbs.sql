{{ config(materialized='table') }}

with fruits as (
    select fruit_id, name from fruits_data.fruits
),
orders as (
    select name, fruit_id, happy, review
    from order_data.orders
)

select fruits.name as fruit, orders.name as customer, review
from fruits join orders
on fruits.fruit_id = orders.fruit_id
