DROP TABLE IF EXISTS mushrooms;
CREATE TABLE mushrooms (
    mushroom_id int,
    name varchar(20),
    discovery datetime,
    friendly bool,
    binary_data binary(7),
    varbinary_data varbinary(16),
    PRIMARY KEY (mushroom_id)
);

INSERT INTO mushrooms VALUES (1, 'portobello', STR_TO_DATE('11/11/2012 8:06:26 AM', '%e/%c/%Y %r'), true, 'bintst', INET6_ATON('127.0.0.1'));
INSERT INTO mushrooms VALUES (2, 'deathcap', STR_TO_DATE('17/3/2018 8:06:26 AM', '%e/%c/%Y %r'), false, '\0\0\1\2\3', INET6_ATON('127.0.0.1'));
