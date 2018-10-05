.. _sgfile:

================================
Using SGFiles to define datasets
================================

SGFiles are similar to Dockerfiles: each command produces a new commit with a deterministic hash that depends
on the current hash and the particulars of a command that's being executed.

The SGFile is parsed using parsimonious, a Python parser library. The exact grammar is in
`splitgraph.sgfile.SGFILE_GRAMMAR` and there is some quality-of-life preprocessing that gets done to the file before
it's interpreted:

  * Newlines can be escaped to make a command multiline (`"\\n"` gets replaced with `""`)
  * Parameters are supported (`${PARAM}` is replaced with the value of the parameter that's either passed
    to the `execute_commands` as a dict or to the commandline `sg file` as a series of arguments
    (`-a key1 val1 -a key2 val2...`)).

The following commands are supported by the interpreter:

Basing an image on another image
--------------------------------

`FROM mountpoint[:tag] [AS alias]`
    Bases the output of the sgfile on a certain revision of the remote/local repository.
    If `AS alias` is specified, the repository is cloned into `alias` and the current contents of `alias` destroyed.
    Otherwise, the current output mountpoint (passed to the executor) is used.

`FROM` can also be used to perform Docker-like multistage builds.

For example::

    FROM internal_data:latest AS stage_1
    CREATE TABLE visible_staff AS SELECT name, age FROM staff WHERE is_restricted = FALSE

    FROM EMPTY AS stage_2
    FROM stage_1 IMPORT {SELECT * FROM visible_staff} AS visible_staff

Importing tables from another image
-----------------------------------

`FROM (mountpoint[:tag])/(MOUNT handler conn_string handler_options) IMPORT table1/{query1} [AS table1_alias], [table2/{query2}...]`
    Uses the `sg import` command to import one or more tables from either a local mountpoint, a remote one, or an
    FDW-mounted database.

Optionally, the table name can be replaced with a SELECT query in curly braces that will get executed against the
source mountpoint in order to create a table. This will be stored as a snapshot. For example:

`FROM internal_data:latest IMPORT {SELECT name, age FROM staff WHERE is_restricted = FALSE} AS visible_staff`
    Will create a new table that contains non-restricted staff names and ages in `internal_data.staff` without including
    any other entries in the table history.

    In the case of imports from FDW, the commit hash produced by this command is random. Otherwise, the commit hash will be
    a combination of the current `OUTPUT` hash, the hash of the source mountpoint and the hashes of the names
    (or source SQL queries) and aliases of all imported tables.

This is crude, but means that the layer is invalidated if there's a change on the remote or we import a different
table/name it differently/use a different query to create a table.  We can improve on this by perhaps only considering
the objects and table aliases that are actually imported (as opposed to the source image hash: maybe the tables
we're importing haven't changed even if other parts of the mountpoint have).


Repository lookups
------------------

Currently, a repository name (mountpoint) is converted to a connection string as follows:

  * See if it exists locally (in the case of the sgfile executor). If it does, try to pull it (to update) and
    use it for `FROM`/`IMPORT` commands.
  * If not, see if it's specified in the `SG_REPO_LOOKUP_OVERRIDE` parameter which has the format
    `repo_1:user:pwd@host:port/db,repo_2:user:pwd@host:port/db...`. Return the matching connection string directly
    without testing to see that the repository exists there.
  * If not, scan the `SG_REPO_LOOKUP` parameter which has the format `user:pwd@host:port/db,user:pwd@host:port/db...`,
    stopping at the first remote that has it.

Running SQL statements
----------------------

`SQL command`
    Runs a (potentially arbitrary) SQL statement. Doesn't enforce any constraints on the SQL yet,
    but the spirit of this command is performing actions on tables in the current `OUTPUT` mountpoint (the command is
    executed with the `OUTPUT` schema being the default one) and not changing/reading data from any other schemas.

The image hash produced by this command is a combination of the current `OUTPUT` hash and the hash of the
"canonicalized" SQL statement (all lowercase with excess whitespace removed). This might cause issues with us not
invalidating layers when an identifier inside a command changed case: then the results would change but the image
returned by the command would still have the same hash.

In the future, it might be worth basing the new hash on the hash of the objects that the query actually interacts with
(as inputs or outputs), but this will require actually parsing the query.
