CREATE TABLE production_data.customers (
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    registration_time TIMESTAMP
);

CREATE TABLE production_data.orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    quantity INTEGER,
    item_name VARCHAR,
    placed_time TIMESTAMP
);
