=================
Command reference
=================

These are all the commands available in the commandline Splitgraph client. Where available, their API
counterparts have also been linked.


Managing images
===============

`sgr commit`
    Also see :mod:`splitgraph.commands.commit`

    Creates a new commit hash (currently picked at random, unless explicitly specified by the SGFile executor or the user)
    and records all changes to the tables in a given schema.

`sgr checkout`
    checks out a given commit into the schema, first deleting any uncommitted chances. Then,
    every table in the given Splitgraph image is materialized (copied into the repository as an actual table).

    As a part of this process, extra physical objects that are required to materialize the image can be downloaded.

    See :mod:`splitgraph.commands.checkout` for the detailed documentation.

Inspecting the status of a repository
=====================================

There are various (commandline and API) commands that can be used to inspect the status of a repository.

:mod:`splitgraph.meta_handler` contains more low-level commands that fetch data directly from the metadata
tables without processing it.

`sgr show REPOSITORY IMAGE_HASH`
    Outputs the information about a given image. The verbose mode (`-v`) also lists all the actual objects
    the image depends on.

`sgr diff REPOSITORY IMAGE_HASH_1 [IMAGE_HASH_2]`
    Also see: :mod:`splitgraph.commands.diff`

    Shows the difference between two images in a repository. If the two images are on the same path in `images`, it
    concatenates their DIFFs and displays that (or the aggregation of total inserts/deletes/updates).
    Note this might give wrong results if there's been a schema change.

    If the images are on different branches), it temporarily materializes both revisions and compares them row-by-row.

`sgr log REPOSITORY`
    Also see: :func:`splitgraph.commands.misc.get_log`

    Returns the log of changes to a given repository, starting from the current HEAD revision and crawling down.
    If `--tree` (`-t`) is passed, outputs the full image tree of the schema.
    Otherwise, and if nothing in the repository is checked out, raises an error.

`sgr status`
    Lists the currently mounted schemata and their checked out images (if any).

Fetching and pushing repositories
=================================

Also see :mod:`splitgraph.commands.push_pull`

`sgr pull`
    Currently is a shorthand for `clone` that uses the remote name specified in `splitgraph_meta.remotes` instead of
    a full connection string.

`sgr clone`
    Brings the metadata for the local repository up to date with a remote one, optionally downloading the actual
    physical objects.

`sgr push`
    Does the opposite.

`sgr publish`
    TODO


Importing tables across repositories
====================================

`sgr import SOURCE_REPOSITORY SOURCE_TABLE TARGET_REPOSITORY [TARGET_TABLE] [SOURCE_IMAGE_OR_TAG]`
    Grafts one or more tables from one repository into another, creating a new single commit on top of the current HEAD.
    This doesn't explicitly preserve the imported tables' history. If the new table(s) isn't/aren't materialized, this
    doesn't consume extra space apart from the new entries in the metadata tables. It also doesn't discard any pending
    changes.

The API version of this function in :mod:`splitgraph.commands.importing` also allows more sophisticated importing,
such as importing only some data from the source table or simply copying the table over without its physical history.

This command usually doesn't need to used directly and is instead called by the :ref:`SGFile interpreter<sgfile>`.

Mounting foreign databases
==========================

See also :mod:`splitgraph.commands.mounting`.

`sgr mount`
    Uses the Postgres FDW to mount a foreign Postgres/Mongo database as a set of tables into a temporary location
    and then imports those tables into the target repository as a new Splitgraph image.

`sgr unmount`
    Destroys the local copy of a repository and all the metadata related to it in
    `images`, `tables`, `remotes` and `snap_tags`. This command doesn't delete the actual physical objects in
    `splitgraph_meta` or references to them in
    `objects` / `object_locations`. There's a separate function, `sgr cleanup`
    (or :func:`splitgraph.commands.misc.cleanup_objects`) that crawls the `splitgraph_meta` for objects not required
    by a current repository and does that.

`sgr init`
    Creates an empty repository with one single initial commit (hash `000000...`).

Custom mount handlers
---------------------

:mod:`splitgraph.commands.mount_handlers`

It is possible to write custom mount handlers for Splitgraph that create foreign tables using the Postgres FDW. A
mount handler is a function that takes a Psycopg connection object, the target schema and any handler-specific
keyword arguments that are passed directly from the user to the handler.

For an example, see the Postgres mount handler in :func:`splitgraph.commands.mount_handlers.mount_postgres`

Provenance tracking
===================

Every :ref:`SGFile command <sgfile>` is recorded in the image metadata so that it's possible to track which datasets an
image depends on, as well as how it can be recreated. Images that are created by `MOUNT` commands
(data import from a mounted database) aren't currently supported, as it's assumed that those databases
aren't publicly accessible.

Provenance tracking allows Splitgraph to recreate the SGFile the image was made with, as well as rebase the image to
use a different version of the datasets it was made from.

`sgr provenance REPOSITORY IMAGE_OR_TAG`
    Inspects the image's parents and outputs a list of datasets and their versions
    that were used to create this image (via `IMPORT` or `FROM` commands). If the `-f (--full)` flag is passed, then the
    command will try to reconstruct the full sgfile used to create the image, raising an error if there's a break in the
    provenance chain (e.g. the `MOUNT` command or a SQL query outside of the sgfile interpreter was used somewhere
    in the history of the image). If the `-e` flag is passed, the command will instead stop at the first break in the chain
    and base the resulting sgfile before the break (using the `FROM` command).

`sgr rerun REPOSITORY IMAGE_OR_TAG -i DATASET1 IMAGE_OR_TAG1 -i ...`
    Recreates the SGFile used to derive a given image
    and reruns it, replacing its dependencies as specified by the `-i` options. If the `-u` flag is passed, the image
    is rederived based on the `latest` tag of all its dependencies.

    For example, if `pgderiv:v1` was created with `pgorigin:v1` and `pgorigin` has been updated on the remote to tag `v2`,
    then both `sgr rerun pgderiv v1 -i pgorigin v2` and `sgr rerun -u pgderiv v1` will have the same effect of rerunning
    the sgfile used to create `pgderiv` based on the latest version of `pgorigin`.
