==========
Quickstart
==========

A normal Splitgraph installation consists of two components: the Splitgraph engine and the Splitgraph client.

The engine is a Docker container with a customized version of PostgreSQL running on it. This is where cached images
are stored.

The client is a Python commandline tool that interacts with the engine.

Most functionality is implemented in the client and to any other application, the engine is just another PostgreSQL
database: it can interact with it by querying tables and writing to them as usual.

To run this demo, you will need to either be able to run Docker (to use the official Splitgraph engine) or have access
to a PostgreSQL (> 9.6) database (in which case some functionality won't be available).

Installing and configuring Splitgraph
=====================================

sgr, the Splitgraph command line client and library can be installed either from
`GitHub <https://github.com/splitgraph/splitgraph/>`_ or from pip::

    pip install --index-url https://test.pypi.org/simple/ splitgraph


Official Splitgraph engine
--------------------------

Use Docker to pull and start the `engine <https://hub.docker.com/r/splitgraph/driver/>`_::

    docker run -d \
    -e POSTGRES_PASSWORD=supersecure \
    -p 5432:5432 \
    splitgraph/driver

By default, ``sgr`` is configured to speak to the engine running on ``localhost:5432`` with a superuser account
called ``sgr`` and a password ``supersecure`` against a database called ``splitgraph``. To complete the installation, run::

    sgr init

Local Postgres
--------------

If you don't want or can't use Docker, you can also run ``sgr`` against any other Postgres database. This will mean
that you won't be able to push/pull images to other Splitgraph engines or mount other databases (since it requires
the Postgres Foreign Data Wrapper extensions to be installed), but it won't prevent you from following the instructions
in this demo.

After installing ``sgr``, run the configurator to generate a base config file::

    sgr config --config-format --no-shielding | tee .sgconfig
    [defaults]
    SG_NAMESPACE=sg-default-ns
    SG_ENGINE_HOST=localhost
    SG_ENGINE_PORT=5432
    SG_ENGINE_DB_NAME=splitgraph
    SG_ENGINE_USER=sgr
    SG_ENGINE_PWD=supersecure
    SG_ENGINE_ADMIN_USER=sgr
    SG_ENGINE_ADMIN_PWD=supersecure
    SG_ENGINE_POSTGRES_DB_NAME=postgres
    SG_CONFIG_FILE=.sgconfig
    SG_META_SCHEMA=splitgraph_meta
    SG_CONFIG_DIRS=
    SG_CONFIG_DIR=
    SG_REPO_LOOKUP=
    SG_REPO_LOOKUP_OVERRIDE=
    SG_S3_HOST=localhost
    SG_S3_PORT=9000
    SG_S3_KEY=
    SG_S3_PWD=

Open ``.sgconfig`` and change ``SG_ENGINE_HOST``, ``SG_ENGINE_PORT``, ``SG_ENGINE_DB_NAME``, ``SG_ENGINE_USER`` and
``SG_ENGINE_PWD`` appropriate to your Postgres installation. Finally, initialize the engine::

    sgr init
    2019-01-01 15:21:43,794 INFO Creating database splitgraph
    2019-01-01 15:21:44,416 INFO Installing the audit trigger...
    2019-01-01 15:21:44,500 INFO Ensuring metadata schema splitgraph_meta exists...

Splitgraph reads its configuration from the overridden environment variables first, then the ``.sgconfig`` file in the
local working directory (unless overridden by the ``SG_CONFIG_FILE`` environment variable, then uses the default values.
For more information, see the documentation for :mod:`splitgraph.config`.

Introduction to repositories
============================

A Splitgraph repository is a collection of images, which themselves are collections of tables. If you're familiar with
Git, you can treat an image as a Git commit and a table -- as the version of a file in a certain commit. Much like
Git, Splitgraph allows you to create database images, track changes to them and check them out.

In this quickstart, we'll create a couple of example repositories and some images and then use the Splitfile language
to define a reproducible transformation on these datasets.

``sgr`` comes with a few routines to set up repositories with some example data. Let's do that right now::

    sgr example generate example/repo_1

This creates a single repository, ``example/repo_1``, with a single table in it called ``demo``. The table has two columns,
``key`` (an integer) and ``value`` (a random hexadecimal string).

In addition, ``sgr`` also checks the repository out into a PostgreSQL schema with the same name. This means that any
application that can access the engine via a normal database connection can interact with the checked-out repository.

``sgr`` provides a shorthand, ``sgr sql`` to run arbitrary SQL queries against the engine::

    sgr sql --schema example/repo_1 "SELECT * FROM demo"

If you run this, you should see 10 rows of data.

You can also inspect the currently checked out image in-depth::

    sgr show --verbose example/repo_1:latest

    Image example/repo_1:5fe74f282e33fb78dda67b3c96f9f915e949d06a643d0b50ce5f74f35ad1e3c7

    Created at 2019-01-01T14:07:57.042234
    Parent: 0000000000000000000000000000000000000000000000000000000000000000

    Tables:
      demo: o8781a2d30bc731a00650ff9833a5bcc65d22597059f5d619dfe3549a7325a9 (SNAP)

This image has a parent with hash ``00000...``, denoting an empty image, and one table, ``demo``. This table is
mapped to an object of type ``SNAP``, meaning that it's stored as a full table snapshot.

Repository manipulation
=======================

A checked out repository also has change tracking enabled, so any alterations to that schema by any application
will be captured, allowing Splitgraph to package them into new images. In particular, changes to preexisting tables
will be stored as ``DIFF`` objects (delta compressed) so that keeping track of table history is space efficient.

Let's create another example repository and give the new image a tag::

    sgr example generate example/repo_2
    sgr sql --schema example/repo_2 "SELECT * FROM demo ORDER BY key"
    sgr tag example/repo_2 original_data

The next step can be done from any application that can access the PostgreSQL database backing the Splitgraph engine,
but ``sgr`` provides a shorthand to alter the generated image::

    sgr example alter example/repo_2
    Deleting 2 rows...
    Updating 2 rows...
    Adding 2 rows...

Let's inspect the current state of the repository::

    sgr sql --schema example/repo_2 "SELECT * FROM demo ORDER BY key"

You should see that the first two rows are now missing, the second two rows have been altered and two more rows
have been added.

Since the checked out repository has change tracking enabled, we can see the exact changes that have been performed to
the ``demo`` table without having to actually compare it to its previous version::

    sgr diff example/repo_2 --verbose
    Between 6881def87f34 and the current working copy:
    demo: added 2 rows, removed 2 rows, updated 2 rows.
    (0,): - None
    (1,): - None
    (2,): U {'c': ['value'], 'v': ['cd36823ad91bf9f972efd98bbfcc30369e1c76ff993cc8ce79c5585535ad6d75']}
    (3,): U {'c': ['value'], 'v': ['77414cb9c351690c63bab539e8170562f4140d18f35d39c9cf3750285cccc4a3']}
    (10,): + {'c': ['value'], 'v': ['a6a43b6c7d4a6c801e3a56a15fafac15e0da62bf489e9e94f3edf24793df2798']}
    (11,): + {'c': ['value'], 'v': ['10a3906410fffe957b8bf577a1eb4f9d60e9b8668dda320235b469ef2de7ac34']}

Let's commit the changes to the new repository to create a new image::

    sgr commit example/repo_2 --message "My first image"
    sgr show --verbose example/repo_2:latest
    Image example/repo_2:a0057ef93849ce853b3c2a7268814cabdb6ba733cfd1f3b23a19ea719e11a98d
    My first image
    Created at 2019-01-01T14:23:33.641988
    Parent: 6881def87f34aa1d7501d2960719fd6b5df9c392053278eade089b3186ee8407

    Tables:
      demo: obd90d0188367a0d9c1b06dff92a729a97d360d50c9fc94438b1b70d71842a5 (DIFF)

You'll see that the ``demo`` table is stored as a ``DIFF`` object in this image: only the 6 changed rows have been
stored in it, as opposed to the whole table.

Tag the new image and check out the old one::

    sgr tag example/repo_2 new_data
    sgr checkout example/repo_2:original_data
    sgr sql --schema example/repo_2 "SELECT * FROM demo ORDER BY key"

You should see the original contents of the table before all changes. Check out the new copy::

    sgr checkout example/repo_2:new_data
    sgr sql --schema example/repo_2 "SELECT * FROM demo ORDER BY key"

Behind the scenes, this replays the changes packaged up in the ``DIFF`` object against the original copy of the table.

Using Splitfiles
================

Whilst you can create new images using ``sgr commit``, there is a better way: Splitfiles. Splitfiles are a
declarative way to define new datasets inspired by Dockerfiles. The same ideas hold here: every command defines a new
image with a deterministic hash (a combination of the hash of the previous image and the specifics of the command). If
the hash that's about to be produced by a command already exists, the image is checked out instead, avoiding the actual
data transformation.

This means that you can not only easily produce derivative datasets, but also keep them up to date without extra effort.

In this demo, we'll run an SQL JOIN query on the two repositories that we produced to match up the keys from
the first table with the keys from the second table.

``sgr`` comes with a generator for this example Splitfile::

    sgr example splitfile example/repo_1 example/repo_2 | tee example.splitfile

    # Import the table from the latest image in the first repository
    FROM example/repo_1 IMPORT demo AS table_1

    # Import the table from a certain image (passed in as a parameter) in the second repository
    FROM example/repo_2:${IMAGE_2} IMPORT demo AS table_2

    # Create a join table
    SQL CREATE TABLE result AS SELECT table_1.key, table_1.value AS value_1,\
                                      table_2.value AS value_2\
                               FROM table_1 JOIN table_2\
                               ON table_1.key = table_2.key

This file consists of three commands:

  * Import the first ``demo`` table into the new image and call it ``table_1``. One thing to note is that importing
    doesn't actually consume extra space in the image, since it just links the image to the original object
    representing the first ``demo`` table. Since we didn't specify an explicit image or tag, the latest image
    in ``example/repo_1`` will be used.
  * Do the same with the ``example/repo_2`` repository, but this time use a parameter ``IMAGE_2`` for the image.
    This parameter will have to be substituted in at Splitfile execution time to define the exact image we
    will want to use.
  * Finally, run an SQL statement against the newly built image with the two imported tables. In this case,
    we will create a new table (called ``result``) and put the results of the JOIN query in it.

Let's run this Splitfile using the original data in the ``example/repo_2`` repository::

    sgr build example.splitfile -o example/output --args IMAGE_2 original_data
    Executing Splitfile example.splitfile with arguments {'IMAGE_2': 'original_data'}

    Step 1/3 : FROM example/repo_1 IMPORT demo AS table_1
    Resolving repository example/repo_1
    Importing tables ('demo',):5fe74f282e33 from example/repo_1 into example/output
     ---> 73c258b9a244

    Step 2/3 : FROM example/repo_2:original_data IMPORT demo AS table_2
    Resolving repository example/repo_2
    Importing tables ('demo',):6881def87f34 from example/repo_2 into example/output
     ---> 9764d69bbc46

    Step 3/3 : SQL CREATE TABLE result AS SELECT table_1.key, table_1.va...
    Executing SQL...
    2019-01-01 14:47:41,368 INFO Committing example/output...
     ---> 96833d33a433

If you have seen the output of a Dockerfile being executed, this will also look familiar to you. Let's inspect the
repository that we've just created::

    sgr log example/output
    H-> 96833d33a4333d33fd2490ca2ec8ebab83be5481cb17372b9a66e1518253a111 2019-01-01 14:47:41.385851 CREATE TABLE result...
        9764d69bbc46429bc897bfe114fefbca9202d39a5d1a6183d65d098133c73096 2019-01-01 14:47:41.302369 Importing ('table_2',) from example/repo_2
        73c258b9a244130f4b5a80adfb9c8c19d0186ca93ba447f2e5a91b68662bb840 2019-01-01 14:47:41.241848 Importing ('table_1',) from example/repo_1
        0000000000000000000000000000000000000000000000000000000000000000 2019-01-01 14:47:41.113029

    sgr show example/output:latest --verbose
    Image example/output:96833d33a4333d33fd2490ca2ec8ebab83be5481cb17372b9a66e1518253a111
    CREATE TABLE result AS SELECT ...
    Created at 2019-01-01T14:47:41.385851
    Parent: 9764d69bbc46429bc897bfe114fefbca9202d39a5d1a6183d65d098133c73096

    Tables:
      table_1: o8781a2d30bc731a00650ff9833a5bcc65d22597059f5d619dfe3549a7325a9 (SNAP)
      table_2: od5a45d033903aa9f8241e5d87e884360fcee56ca874ae12c7cc94942b84092 (SNAP)
      result: o4be220f929a52b1c1eaa2c083a1fbcab0cbc4bdd3c9a634e6a331dbe92b990 (SNAP)

Let's also look at the actual data we produced. Since we ran the Splitfile against the original version of the
``example/repo_2`` repository, all keys from 0 to 9 should be present in the join table::

    sgr sql --schema example/output "SELECT * FROM result ORDER BY key"
    [(0,
      'e3f8da4d6d906fc5b7d33a670b2b370bbe617c30a9b4b56cda901605b8cbc014',
      'da6b3aca32bbc1011aff3c57e453ab9e4e1be1f7373fc1f5a745d19e4f70d955'),
     (1,
      '649e8e3fd3c2afafafbdd1b822030f70e5068417bfb8575fce23419ea4eb2ff',
      '980766c6ad7d50f0abd0b23c52b88641c2cef27c69d8ae91387d31eafc4f5ef4'),
     (2,
      '1d8cdaef663c05d4792c692bfe3344d77935bd6d04ccf23f508dec2ae4f156fb',
      '53193b48f075f61de3be0d8c344d1795f4d5e69760cadb9a255fd7afd638cc7f'),
     (3,
      'c720af85b57634711987d4cc21bdecb7c68f8e76f7a39f3d9c5ffb500528f3e6',
      '9524f629b9fcf93db71a3f782761e3d197cb8ccf055cb47ed56547909c110938'),
     (4,
      'c7b40b97aff78b36ecaf694bb17f45d6779fb9ab35e7e8691028cc2b4a33d2c7',
      '2101df664248a539a9cb66c1e598211e1d11e485089e32583fff2d9ad3487993'),
     (5,
      '9f945d56c6df763a54f463a36e5139592c5cd50b74cd817e0aff29ec13694a9',
      '30ddd6e11c61e3242ebbfb9d4eee426da0cf2b1e6ea8dc135b5fce3d5757e02'),
     (6,
      '77c236d7da1a04c42a353a83b57225f54e48afead36610764c4021e06611f918',
      'efbea91e4e8ca2fbe7c7923896c90ae30092b409b7a796993c4da918f1e522ab'),
     (7,
      'c9fa6479320a5bd1cc7900e68f07ae81b11c1e7d1f7f337096ef28b60026fd8c',
      '1738b2c8c1b2a7a0e5bd09dfea8e232dec8b626224ceb62dd4e54d485b71957d'),
     (8,
      '2397a04861b9ee87057ed023a2a5f10ec4ed1c14184f22cbf53606783398e1fa',
      '8edd06b686969ca768a6d7ee77bad780ceb0217c3a61ab18e65ab0f840d438b'),
     (9,
      '500fd35e1fb72c4d0b5d1060734964b0e338f273ee41567cae51158ce98cfde1',
      'd6cb4f591a28d27d7a041be13cf00408d7e2f0d989e6a32f88d0d9259d1bab2a')]

Finally, since each image at execution time has a deterministic hash, rerunning the same Splitfile won't actually
perform any computation::

    sgr build example.splitfile -o example/output --args IMAGE_2 original_data
    Executing Splitfile example.splitfile with arguments {'IMAGE_2': 'original_data'}

    Step 1/3 : FROM example/repo_1 IMPORT demo AS table_1
    Resolving repository example/repo_1
     ---> Using cache
     ---> 73c258b9a244

    Step 2/3 : FROM example/repo_2:original_data IMPORT demo AS table_2
    Resolving repository example/repo_2
     ---> Using cache
     ---> 9764d69bbc46

    Step 3/3 : SQL CREATE TABLE result AS SELECT table_1.key, table_1.va...
     ---> Using cache
     ---> 96833d33a433

For more information on the available Splitfile commands, see :ref:`splitfile`.

Advanced Splitfile usage
========================

There's one other advantage to using Splitfiles instead of building images manually: provenance tracking. An image
that is created by a Splitfile has the specifics of the actual command that created it stored in its metadata.

This means that we can output a list of images and repositories that were used in an image's creation::

    sgr provenance example/output:latest
    example/output:96833d33a4333d33fd2490ca2ec8ebab83be5481cb17372b9a66e1518253a111 depends on:
    example/repo_2:6881def87f34aa1d7501d2960719fd6b5df9c392053278eade089b3186ee8407
    example/repo_1:5fe74f282e33fb78dda67b3c96f9f915e949d06a643d0b50ce5f74f35ad1e3c7

Since the Splitfile in the previous section imported tables from the two generated repositories, these repositories
are considered to be ``example/output``'s dependencies (note that the ``example/output`` repository can still be
checked out and used without those two repositories present).

However, that's not all. We can also reconstruct a Splitfile that can be used to rebuild the newly created image,
without the original Splitfile present::

    sgr provenance example/output:latest --full
    # Splitfile commands used to recreate example/output:96833d33a4333d33fd2490ca2ec8ebab83be5481cb17372b9a66e1518253a111
    FROM example/repo_1:5fe74f282e33fb78dda67b3c96f9f915e949d06a643d0b50ce5f74f35ad1e3c7 IMPORT demo AS table_1
    FROM example/repo_2:6881def87f34aa1d7501d2960719fd6b5df9c392053278eade089b3186ee8407 IMPORT demo AS table_2
    SQL CREATE TABLE result AS SELECT table_1.key, table_1.value AS value_1, table_2.value AS value_2 FROM table_1 JOIN table_2 ON table_1.key = table_2.key

Unlike Dockerfiles and building filesystem images, in this case everything that is needed to rebuild a certain image
(links to source images and the actual commands) is encoded in the provenance information. In particular, this means
that we can rebuild the ``example/output:latest`` image against a different version of any of its dependencies by simply
substituting a different image hash into this regenerated Splitfile::

    sgr rebuild example/output:latest --against example/repo_2:new_data
    Rerunning example/output:96833d33a4333d33fd2490ca2ec8ebab83be5481cb17372b9a66e1518253a111 against:
    example/repo_2:new_data

    Step 1/3 : FROM example/repo_1:5fe74f282e33fb78dda67b3c96f9f915e949d...
    Resolving repository example/repo_1
     ---> Using cache
     ---> 73c258b9a244

    Step 2/3 : FROM example/repo_2:new_data IMPORT demo AS table_2
    Resolving repository example/repo_2
    Importing tables ('demo',):a0057ef93849 from example/repo_2 into example/output
    2019-01-01 15:07:49,632 INFO Applying obd90d0188367a0d9c1b06dff92a729a97d360d50c9fc94438b1b70d71842a5...
     ---> bc8791660f6d

    Step 3/3 : SQL CREATE TABLE result AS SELECT table_1.key, table_1.va...
    Executing SQL...
    2019-01-01 15:07:49,695 INFO Committing example/output...
     ---> a2c37225c2d1

Since the same image from ``example/repo_1`` is used here, the first step in the execution results in the same image hash
and so the image is simply checked out. However, since the image from ``example/repo_2`` is now the altered one that
we created in the previous section, the rest of the derivation has to be rerun.

Let's examine the new result::

    sgr sql --schema example/output "SELECT * FROM result ORDER BY key"
    [(2,
      '1d8cdaef663c05d4792c692bfe3344d77935bd6d04ccf23f508dec2ae4f156fb',
      'cd36823ad91bf9f972efd98bbfcc30369e1c76ff993cc8ce79c5585535ad6d75'),
     (3,
      'c720af85b57634711987d4cc21bdecb7c68f8e76f7a39f3d9c5ffb500528f3e6',
      '77414cb9c351690c63bab539e8170562f4140d18f35d39c9cf3750285cccc4a3'),
     (4,
      'c7b40b97aff78b36ecaf694bb17f45d6779fb9ab35e7e8691028cc2b4a33d2c7',
      '2101df664248a539a9cb66c1e598211e1d11e485089e32583fff2d9ad3487993'),
     (5,
      '9f945d56c6df763a54f463a36e5139592c5cd50b74cd817e0aff29ec13694a9',
      '30ddd6e11c61e3242ebbfb9d4eee426da0cf2b1e6ea8dc135b5fce3d5757e02'),
     (6,
      '77c236d7da1a04c42a353a83b57225f54e48afead36610764c4021e06611f918',
      'efbea91e4e8ca2fbe7c7923896c90ae30092b409b7a796993c4da918f1e522ab'),
     (7,
      'c9fa6479320a5bd1cc7900e68f07ae81b11c1e7d1f7f337096ef28b60026fd8c',
      '1738b2c8c1b2a7a0e5bd09dfea8e232dec8b626224ceb62dd4e54d485b71957d'),
     (8,
      '2397a04861b9ee87057ed023a2a5f10ec4ed1c14184f22cbf53606783398e1fa',
      '8edd06b686969ca768a6d7ee77bad780ceb0217c3a61ab18e65ab0f840d438b'),
     (9,
      '500fd35e1fb72c4d0b5d1060734964b0e338f273ee41567cae51158ce98cfde1',
      'd6cb4f591a28d27d7a041be13cf00408d7e2f0d989e6a32f88d0d9259d1bab2a')]

Since the first two rows are now missing from ``example/repo_2``, the JOIN result doesn't have them either.

The caching behaviour still holds here, so if we do a ``rebuild`` against the original version of ``example/repo_1``,
the first version of ``example/output`` will be checked out::

    sgr rebuild example/output:latest --against example/repo_2:original_data
    Rerunning example/output:a2c37225c2d162c0d78bc15195a740e3a231bb9c1b5f37c00d3bf04560c63216 against:
    example/repo_2:original_data

    Step 1/3 : FROM example/repo_1:5fe74f282e33fb78dda67b3c96f9f915e949d...
    Resolving repository example/repo_1
     ---> Using cache
     ---> 73c258b9a244

    Step 2/3 : FROM example/repo_2:original_data IMPORT demo AS table_2
    Resolving repository example/repo_2
     ---> Using cache
     ---> 9764d69bbc46

    Step 3/3 : SQL CREATE TABLE result AS SELECT table_1.key, table_1.va...
     ---> Using cache
     ---> 96833d33a433

