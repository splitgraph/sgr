CREATE SCHEMA production_data;
CREATE TABLE production_data.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR,
    registration_time TIMESTAMP
);

CREATE TABLE production_data.orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    quantity INTEGER,
    item_name VARCHAR,
    placed_time TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES production_data.customers(id)
);

INSERT INTO production_data.customers(name, registration_time) VALUES
    ('Jack Doe', '2020-01-01 12:00:00'),
    ('Jane Doe', '2020-01-02 03:04:00'),
    ('Alexandra Sample', '2020-03-01 01:05:01');

INSERT INTO production_data.orders (customer_id, quantity, item_name, placed_time) VALUES
    (1, 15, 'Toilet Roll', '2020-03-15 12:00:00'),
    (1, 20, 'Hand Sanitizer', '2020-03-16 02:00:00'),
    (2, 5, 'Pasta', '2020-03-21 17:32:11'),
    (3, 50, 'Surgical Mask', '2020-04-01 12:00:01'),
    (1, 50, 'Surgical Mask', '2020-04-02 11:29:42');
