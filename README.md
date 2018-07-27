Installation:

```
conda env update (or pip install psycopg2 click)
python setup.py install
```

then use `sg mount/unmount/commit/checkout/status/log/sql/file/diff/pull/push` commands.

Some example uses in do\_stuff.sh. Currently, it assigns a random "hash" (instead of actually hashing the table contents / queries done -- apart from executing an sgfile where the hash is `hash(concatenate(all source hashes, current output head, hash of the SQL query)))` to each schema layer. It can either copy all tables during a commit or diff them against the last commit and store that instead.

Limited/broken things:

  * Only Postgres/Mongo FDW are supported. On cloning, the whole target schema/table gets copied over.
  * Postgres has a 63 character limit on table names, so tables are stored as "git objects" in the mounted schema (pack/snapshots) and there's an indirection in the meta schema mapping the table name and snap ID to the actual table.
  * Storing committed tables as diffs only supported for when there were no schema changes between versions (otherwise a snapshot is needed).
  * Diffing is slow: the code assumes there's no pk/indices on the diffed tables and literally fetches both tables and diffs the tuples in n^2. It also assumes all tuples are distinct.
  * The code also doesn't make any assumptions about whether the series of hashes between two snapshots of interest has diffs or actual materialized snapshots in it: to produce a diff of two hashes, it checks out both into a temporary set of tables and does the diff that way.
  * No writing back to the upstream table.
