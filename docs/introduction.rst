.. _introduction:

============
Introduction
============

What if you could `git push` your database?

Splitgraph is a data management tool and a data sharing platform inspired by Docker and Git.

Splitgraph allows the user to manipulate data images (snapshots of SQL tables at a given point in time) as if they
were code repositories: by creating, versioning, sharing and downloading them. This is all done seamlessly, so that
any tool that interacts with a Splitgraph table has no idea it's not just an ordinary database table.

Furthermore, Splitgraph also defines an Splitfile language that is used to derive datasets from other datasets whilst
being able to keep track of their provenance and update the image when its dependencies are updated. You can even
plug another database into Splitgraph and query data from it as if it were just another set of tables.

Ever wished you could do this?

::

    FROM noaa/monthly:latest IMPORT state_temperature
    FROM usda/ers:latest IMPORT crop_yields
    FROM MOUNT mongo_fdw mongodb.mycompany.com '{"sales": {"db": "analyst_data",
                                                           "coll": "monthly_pnl",
                                                           "schema": {"month": "timestamp",
                                                                      "state": "varchar",
                                                                      "sales": "numeric"}}}'
         IMPORT sales
    SQL CREATE TABLE staging AS SELECT st.timestamp, st.state, st.temperature, yield, sales\
                                FROM state_temperature st JOIN crop_yields cy JOIN sales sl\
                                ON st.timestamp = cy.timestamp\
                                AND st.state = cy.state\
                                AND cy.state = sl.state

Now you can.

Finally, you can share your datasets on `<http://registry.splitgraph.com/>`_ or on your own private registry.