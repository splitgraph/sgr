DROP TABLE IF EXISTS fruits;

CREATE TABLE fruits (
    fruit_id serial primary key,
    name varchar
);

INSERT INTO fruits (name) VALUES ('apple');
INSERT INTO fruits (name) VALUES ('orange');
INSERT INTO fruits (name) VALUES ('tomato');
