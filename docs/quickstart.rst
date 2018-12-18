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

First, pull and start the `driver
<https://hub.docker.com/r/splitgraph/driver/>`_::

    docker run -d \
    -e POSTGRES_PASSWORD=supersecure \
    -p 5432:5432 \
    splitgraph/driver

Then, install the Splitgraph `client
<https://github.com/splitgraph/splitgraph/>`_::

    pip install splitgraph

**NB this doesn't work yet** -- see :ref:`introduction` / other docs for the vision/overview. In this case, clone
the ``sgr`` client from Github and install it manually.

Finally, initialize the driver and pull some data::

    sgr init
    sgr pull noaa/monthly
    sgr checkout noaa/monthly:201801

The ``noaa/monthly`` schema on the driver now contains the January 2018 version of the NOAA monthly climate data, queryable
by any tool that understands SQL. You can also run an ad hoc query yourself::

    sgr sql -s noaa_monthly "SELECT temperature FROM state_temperature WHERE state = 'AZ'"

Why January 2018? We have no idea. Let's update your copy of the data::

    sgr checkout noaa/monthly:latest

This downloads just the parts of this dataset that were changed, so keeping your data up to date is easy.

Changing and pushing data
=========================

Let's manipulate our copy of the climate data a bit::

    sgr sql -s noaa/monthly "DELETE FROM state_temperature WHERE state = 'AZ'"

Alternatively, we can connect any other Postgres client to your driver, since it's just another database server.

Now, inspect the changes we've made::

    sgr diff noaa/climate -v

And commit and tag them. Let's also tag the previous version so that we don't lose track of it::

    sgr tag noaa/climate with_az
    sgr commit noaa/climate -m "Removed Arizona from the data"
    sgr tag noaa/climate without_az

Finally, push them back upstream. Since we don't have write access to the ``noaa`` namespace, we'll use your personal
namespace for this::

    sgr push noaa/climate <your_username>/climate

Splitfiles
==========

Splitfiles are a way to define transformations on images in a composable, maintainable and reproducible way. They were
inspired and heavily influenced by the simplicity of Dockerfiles, so if you are familiar with those, you should have
no issues with understanding how Splitfiles work.

Let's try producing a derivative dataset by joining the dataset we created in the previous section with the USDA
crop yields data.

First, create the Splitfile::

    # Import the corn yields data into the new image
    FROM usda/ags:latest IMPORT corn_yields

    # Import the altered NOAA climate data
    FROM <your_username>/climate:with_az IMPORT state_rainfall

    # Create a join table
    SQL CREATE TABLE rainfall_yields AS SELECT rainfall.timestamp, rainfall, yield, rainfall.state\
                                    FROM rainfall JOIN crop_yields ON rainfall.state = crop_yields.state\
                                                                   AND rainfall.timestamp = crop_yields.timestamp\
                                    ORDER BY timestamp, state

Then, execute it::

    sgr build join_crops.splitfile -o joined_data

Much like with Dockerfiles, every image has a deterministic hash, so if the upstream data hasn't changed, no new images
will be produced and no calculations will be rerun. Try running the same command to see what happens.

Finally, let's actually look inside the dataset we've created::

    sgr sql -s joined_data "SELECT * FROM rainfall_yields LIMIT 10"


Provenance
==========

Unlike Docker, all context required to build a particular image is stored in the image's metadata. This means that we
can easily find out the exact provenance of an image::

    sgr provenance joined_data:latest

You'll see here that instead of tags (like latest) we've used the actual image hashes.

Moreover, we can also reconstruct a Splitfile that can be used to recreate this image::

    sgr provenance -f joined_data:latest

Note that this is done from the image metadata: if we were to push the image out to the registry and if someone else
pulled it, they would get the same result without having to have our original Splitfile handy.

Finally, with that in mind, Splitgraph can substitute any other image instead of the original images, performing
a kind of a "logical rebase" and allowing us to keep our derivative datasets up to date. Let's instead rerun our dataset
creation against the copy of the climate data without Arizona::

    sgr rebuild joined_data:latest --against <your_username>/climate:without_az
    sgr sql -s joined_data "SELECT * FROM rainfall_yields WHERE state = 'AZ'"

You'll see that a new image was generated without the data for Arizona. Since rerunning is aware of Splitgraph's image
hashing, we can as easily go back to an image based on the data with Arizona without performing any calculations::

    sgr rebuild joined_data:latest --against <your_username>/climate:with_az
    sgr sql -s joined_data "SELECT * FROM rainfall_yields WHERE state = 'AZ'"

We can also rerun our image against the latest versions of all of its dependencies::

    sgr rebuild -u joined_data:latest
