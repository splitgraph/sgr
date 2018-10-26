DROP TABLE IF EXISTS fruits;
CREATE TABLE fruits (
    fruit_id serial,
    name varchar
);

DROP TABLE IF EXISTS vegetables;
CREATE TABLE vegetables (
    vegetable_id serial,
    name varchar
);

INSERT INTO fruits (name) VALUES ('apple');
INSERT INTO fruits (name) VALUES ('orange');

INSERT INTO vegetables (name) VALUES ('potato');
INSERT INTO vegetables (name) VALUES ('carrot');

