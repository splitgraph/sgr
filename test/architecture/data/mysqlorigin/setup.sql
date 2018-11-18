DROP TABLE IF EXISTS mushrooms;
CREATE TABLE mushrooms (
    mushroom_id int,
    name varchar(20),
    discovery datetime,
    friendly bool,
    PRIMARY KEY (mushroom_id)
);

INSERT INTO mushrooms VALUES (1, 'portobello', STR_TO_DATE('11/11/2012 8:06:26 AM', '%e/%c/%Y %r'), true);
INSERT INTO mushrooms VALUES (2, 'deathcap', STR_TO_DATE('17/3/2018 8:06:26 AM', '%e/%c/%Y %r'), false);