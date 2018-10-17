==========
Quickstart
==========

A normal Splitgraph installation consists of two components: the Splitgraph driver and the Splitgraph client.

The driver is a Docker container with a customized version of PostgreSQL running on it. This is where cached images
are stored.

The client is a Python commandline tool that interacts with the driver.

Most functionality is implemented in the client and to any other application, the driver is just another PostgreSQL
database: it can interact with it by querying tables and writing to them as usual.

Installation and quick demo
===========================

**NB this doesn't work yet** -- see :ref:`introduction` / other docs for the vision/overview

First, pull and start the `driver
<https://hub.docker.com/r/splitgraph/driver/>`_::

    docker run -d \
    -e POSTGRES_PASSWORD=supersecure \
    -p 5432:5432 \
    splitgraph/driver

Then, install the Splitgraph `client
<https://github.com/splitgraph/splitgraph/>`_::

    pip install splitgraph

Finally, pull some data::

    sgr pull noaa/monthly
    sgr checkout noaa/monthly 201801

The `noaa/monthly` schema on the driver now contains the January 2018 version of the NOAA monthly climate data, queryable
by any tool that understands SQL. You can also run an ad hoc query yourself::

    sgr sql "SELECT temperature FROM noaa/monthly.state_temperature WHERE state = 'AZ'"

Why January 2018? We have no idea. Let's update your copy of the data::

    sgr checkout noaa/monthly latest

This downloads just the parts of this dataset that were changed, so keeping your data up to date is easy.

Changing and pushing data
=========================

