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

DROP TABLE IF EXISTS account;
CREATE TABLE account (
    account_number integer,
    balance integer,
    firstname character varying(20),
    lastname character varying(20),
    age integer,
    gender character varying(1),
    address text,
    employer character varying(20),
    email text,
    city character varying(20),
    state character varying(5)
);

COPY account from '/src/accounts.csv' DELIMITER ',' CSV HEADER;
