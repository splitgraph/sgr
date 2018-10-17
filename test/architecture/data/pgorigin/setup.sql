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


-- Data for a demo with some real-world application (joins w/ a mongodb)
DROP SCHEMA IF EXISTS demo_data CASCADE;
CREATE SCHEMA demo_data;
CREATE TABLE demo_data.clients (
    client_id VARCHAR,
    name      VARCHAR,
    address   VARCHAR,
    PRIMARY KEY (client_id)
);

CREATE TABLE demo_data.license_status (
    client_id VARCHAR,
    status    VARCHAR,
    status_ts TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES demo_data.clients (client_id)
);

INSERT INTO demo_data.clients VALUES ('JEEX1ABC1', 'James Example', '53 Example Rd, Examplington, EX1 ABC');
INSERT INTO demo_data.clients VALUES ('MTTS5DEF1', 'Maria Test', 'Flat 21, Test House, 27 Test Drive, Testingshire, TS5 DEF');
INSERT INTO demo_data.clients VALUES ('ASSP1GHI1', 'Alex Sample', 'Sample House, Sample Drive, Sampleton, SP1 GHI');

INSERT INTO demo_data.license_status VALUES ('JEEX1ABC1', 'PURCHASED', '2017-08-30 12:00:00');
INSERT INTO demo_data.license_status VALUES ('MTTS5DEF1', 'NOT_REQUIRED', '2017-09-01 01:02:03');