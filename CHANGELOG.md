# Changelog

## v0.3.11 (2022-07-28)

  * Fix bug for Python 3.10 compatibility (https://github.com/splitgraph/sgr/pull/704)
  * `sgr engine add` password prompt clarification (https://github.com/splitgraph/sgr/pull/709)

## v0.3.10 (2022-06-08)

  * Fix surrogate PK object ordering edge case, impacting some LQ mounts (https://github.com/splitgraph/sgr/pull/681).
  * Add new `postgres_fdw` params: `use_remote_estimate` and `fetch_size` (https://github.com/splitgraph/sgr/pull/678)
  * Add support for writable LQ to Splitfiles (https://github.com/splitgraph/sgr/pull/668)

## v0.3.9 (2022-04-26)

Add support for writes when using layered querying (`sgr checkout --layered`) (https://github.com/splitgraph/sgr/pull/662). Note
schema changes when an image is checked out in layered querying mode are currently unsupported.

Also add support for using writeable layered querying when using Splitfiles (https://github.com/splitgraph/sgr/pull/668).

## v0.3.8 (2022-03-18)

  * Fix slow startup of `sgr` binary on Mac OS / Darwin (https://github.com/splitgraph/sgr/pull/656)
    * Add new release artifact `sgr-osx-x86_64.tgz` including executable sgr, shared libraries, and resources
    * Change `install.sh` on Darwin to default to the `.tgz` artifact (for previous behavior, set `FORCE_ONEFILE=1`)
    * Extract the archive to `~/.splitgraph/pkg/sgr` and symlink `~/.splitgraph/sgr -> ~/.splitgraph/pkg/sgr/sgr`
  * Add support for incremental loads to FDW plugins (https://github.com/splitgraph/sgr/pull/647)
    * Add a `cursor_columns` field to the table parameters of FDW data sources
  * Fix bug on Windows where `sgr` failed to locate `.sgconfig` in non-existent `$HOME` directory (https://github.com/splitgraph/sgr/pull/651) _Thanks @harrybiddle!_
    * Switch to cross-platform path expansion when adding home directory to config search paths

Starting from this version, all future releases will include `sgr-osx-x86_64.tgz`, which we recommend installing on Mac OS. The `install.sh` script will default to it.

Note: if you download an executable directly from this release page, using a web browser, then Mac will quarantine the file and refuse to execute it. Command line HTTP clients like `curl` do not have this limitation, and the recommended installation method is to run the `install.sh` script included as a part of every release. See [the pull request adding `sgr-osx-x86_64.tgz`](https://github.com/splitgraph/sgr/pull/656) for more details.

## v0.3.7 (2022-02-28)

  * Add Google BigQuery data plugin (https://github.com/splitgraph/sgr/pull/638)
  * Add Amazon Athena data plugin (https://github.com/splitgraph/sgr/pull/634)
  * Skip pushdown of aggregations with `WHERE` clause downcasting (https://github.com/splitgraph/Multicorn/pull/6)

## v0.3.6 (2022-02-02)

  * Fix `libffi` crashes when using the Snowflake FDW (https://github.com/splitgraph/sgr/pull/623)

## v0.3.5 (2022-01-31)

  * Support for aggregation and GROUP BY pushdown for the Snowflake FDW
    * https://github.com/splitgraph/sgr/pull/620
    * https://github.com/splitgraph/Multicorn/pull/3
  * Fix FDW previews when column names have percentage signs (https://github.com/splitgraph/sgr/pull/619)

## v0.3.4 (2022-01-21)

  * Further support for aggregation pushdown with Multicorn-backed FDWs
    * https://github.com/splitgraph/Multicorn/pull/2
    * https://github.com/splitgraph/sgr/pull/613
    * https://github.com/splitgraph/postgres-elasticsearch-fdw/pull/2
  * Speed up `sgr commit` chunking by about 1.5x-10x (https://github.com/splitgraph/sgr/pull/611)
  * Various fixes to `sgr cloud seed` (https://github.com/splitgraph/sgr/pull/606)

## v0.3.3 (2022-01-05)

 * Minor fixes to `sgr cloud seed` (https://github.com/splitgraph/sgr/pull/604)
 * Add a flag to restore old `sgr csv import` behaviour (https://github.com/splitgraph/sgr/pull/605)

## v0.3.2 (2021-12-30)

  * Fix various issues with the dbt project and GitHub Actions that `sgr cloud seed` generates (https://github.com/splitgraph/sgr/pull/602, https://github.com/splitgraph/sgr/pull/591)
  * Initial support for aggregation pushdown with Multicorn-backed FDWs (currently implemented in our fork of postgres-elasticsearch-fdw)
    * https://github.com/splitgraph/sgr/pull/581
    * https://github.com/splitgraph/Multicorn/pull/1
    * https://github.com/splitgraph/postgres-elasticsearch-fdw/pull/1

## v0.3.1 (2021-12-20)

Fix `sgr cloud sync` invocation issue (https://github.com/splitgraph/sgr/pull/589)

## v0.3.0 (2021-12-17)

Fleshing out the `splitgraph.yml` (aka `repositories.yml`) format that defines a Splitgraph Cloud "project" (datasets, their sources and metadata).

Existing users of `repositories.yml` don't need to change anything, though note that `sgr cloud` commands using the YAML format will now default to `splitgraph.yml` unless explicitly set to `repositories.yml`.

### New `sgr cloud` commands

See https://github.com/splitgraph/sgr/pull/582 and https://github.com/splitgraph/sgr/pull/587

These let users manipulate Splitgraph Cloud and ingestion jobs from the CLI:

  * `sgr cloud status`: view the status of ingestion jobs in the current project
  * `sgr cloud logs`: view job logs
  * `sgr cloud upload`: upload a CSV file to Splitgraph Cloud (without using the engine)
  * `sgr cloud sync`: trigger a one-off load of a dataset
  * `sgr cloud stub`: generate a `splitgraph.yml` file
  * `sgr cloud seed`: generate a Splitgraph Cloud project with a `splitgraph.yml`, GitHub Actions, dbt etc
  * `sgr cloud validate`: merge multiple project files and output the result (like `docker-compose config`)
  * `sgr cloud download`: download a query result from Splitgraph Cloud as a CSV file, bypassing time/query size limits.

### `splitgraph.yml`

Change various commands that use `repositories.yml` to default to `splitgraph.yml` instead. Allow "mixing in" multiple `.yml` files Docker Compose-style, useful for splitting credentials (and not checking them in) and data settings.

Temporary location for the new full documentation on `splitgraph.yml`: https://github.com/splitgraph/sgr.com/blob/f7ac524cb5023091832e8bf51b277991c435f241/content/docs/0900_splitgraph-cloud/0500_splitgraph-yml.mdx

### Miscellaneous

  * Initial backend support for "transforming" Splitgraph plugins, including dbt (https://github.com/splitgraph/sgr/pull/574)
  * Dump scheduled ingestion/transformation jobs with `sgr cloud dump` (https://github.com/splitgraph/sgr/pull/577)

Full set of changes: [`v0.2.18...v0.3.0`](https://github.com/splitgraph/sgr/compare/v0.2.18...v0.3.0)

## v0.2.18 (2021-11-17)

  * Splitfile speedups (https://github.com/splitgraph/sgr/pull/567)
  * Various query speedups (https://github.com/splitgraph/sgr/pull/563, https://github.com/splitgraph/sgr/pull/561)
  * More robust CSV querying (https://github.com/splitgraph/sgr/pull/562)

Full set of changes: [`v0.2.17...v0.2.18`](https://github.com/splitgraph/sgr/compare/v0.2.17...v0.2.18)

## v0.2.17 (2021-10-14)

  * Code refactor / optimizations (https://github.com/splitgraph/sgr/pull/531)
  * Support for pluggable authorization logic (https://github.com/splitgraph/sgr/pull/542, https://github.com/splitgraph/sgr/pull/549)
  * FDW JSONSchema fixes (https://github.com/splitgraph/sgr/pull/545)
  * Upgrade pglast to 3.4 to fix issues with Splitfile validation (https://github.com/splitgraph/sgr/pull/534)
  * Speed up Splitfile builds (https://github.com/splitgraph/sgr/pull/550)

Full set of changes: [`v0.2.16...v0.2.17`](https://github.com/splitgraph/sgr/compare/v0.2.16...v0.2.17)

## v0.2.16 (2021-08-18)

  * Various Airbyte ingestion improvements and support for different normalization modes, including a custom dbt model (https://github.com/splitgraph/sgr/pull/510, https://github.com/splitgraph/sgr/pull/513, https://github.com/splitgraph/sgr/pull/514)
  * Fix mount for data source with empty credentials schema (https://github.com/splitgraph/sgr/pull/515)
  * Fix `sgr cloud load`/`dump` (https://github.com/splitgraph/sgr/pull/520)

Full set of changes: [`v0.2.15...v0.2.16`](https://github.com/splitgraph/sgr/compare/v0.2.15...v0.2.16)


## v0.2.15 (2021-07-26)

  * API functionality to get the raw URL for a data source (https://github.com/splitgraph/sgr/pull/457)
  * LQ scan / filtering simplification to speed up writes / Singer loads (https://github.com/splitgraph/sgr/pull/464, https://github.com/splitgraph/sgr/pull/489)
  * API functionality for Airbyte support (`AirbyteDataSource` class, https://github.com/splitgraph/sgr/pull/493)
  * Speed up `sgr cloud load` by bulk API calls (https://github.com/splitgraph/sgr/pull/500)

Full set of changes: [`v0.2.14...v0.2.15`](https://github.com/splitgraph/sgr/compare/v0.2.14...v0.2.15)

## v0.2.14 (2021-05-05)

  * Functionality to dump and load a Splitgraph catalog to/from a special `repositories.yml` format (https://github.com/splitgraph/sgr/pull/445)

Full set of changes: [`v0.2.13...v0.2.14`](https://github.com/splitgraph/sgr/compare/v0.2.13...v0.2.14)

## v0.2.13 (2021-04-14)

  * Various fixes to CSV inference and querying (https://github.com/splitgraph/sgr/pull/433)
  * Add customizable fetch size to the Snowflake data source (https://github.com/splitgraph/sgr/pull/434)
  * Fix issue with changing the engine password (https://github.com/splitgraph/sgr/pull/437)
  * Data source refactor (https://github.com/splitgraph/sgr/pull/438):
    * MySQL: parameter `remote_schema` has been renamed to `dbname`
    * Mongo: parameter `coll` has been renamed to `collection`; `db` to `database`
    * Table options are now a separate parameter that is passed to the
    * Introspection now returns a dictionary of tables and proposed table options OR error classes for tables that we weren't able to introspect (allowing for partial failures)
    * Mounting can now return a list of mount errors (caller can choose to ignore).
    * CSV data source: allow passing a partially initialized list of table options without a schema, making it introspect just those S3 keys and fill out the missing table options.
  * Postgres-level notices are now available in the `PsycopgEngine.notices` list after a `run_sql` invocation.
  * Multicorn: fix bug where server-level FDW options would override table-level FDW options.

Full set of changes: [`v0.2.12...v0.2.13`](https://github.com/splitgraph/sgr/compare/v0.2.12...v0.2.13)

## v0.2.12 (2021-04-07)

  * Fixes to the Snowflake data source (https://github.com/splitgraph/sgr/pull/421)
  * Add automatic encoding, newline and dialect inference to the CSV data source (https://github.com/splitgraph/sgr/pull/432)

Full set of changes: [`v0.2.11...v0.2.12`](https://github.com/splitgraph/sgr/compare/v0.2.11...v0.2.12)

## v0.2.11 (2021-03-29)

  * Snowflake data source improvements:
    * Allow passing envvars to set HTTP proxy parameters, fix incorrect query string generation when passing a warehouse (https://github.com/splitgraph/sgr/pull/414, https://github.com/splitgraph/sgr/issues/413)
    * Support for authentication using a private key (https://github.com/splitgraph/sgr/pull/418)
  * Splitfiles: relax AST restrictions to support all SELECT/INSERT/UPDATE/DELETE statements (https://github.com/splitgraph/sgr/issues/411)
  * Change the default installation port to 6432 and handle port conflicts during install (https://github.com/splitgraph/sgr/issues/375)
  * Add retry logic to fix registry closing the SSL connection after 30 seconds, close remote connections in some places (https://github.com/splitgraph/sgr/pull/417)

Full set of changes: [`v0.2.10...v0.2.11`](https://github.com/splitgraph/sgr/compare/v0.2.10...v0.2.11)

## v0.2.10 (2021-03-17)

  * Fix CSV schema inference not supporting BIGINT data types (https://github.com/splitgraph/sgr/pull/407)
  * Fix Splitfiles only expecting tags to contain alphanumeric characters (https://github.com/splitgraph/sgr/pull/407)
  * Speedups for the Snowflake / SQLAlchemy data soure (https://github.com/splitgraph/sgr/pull/405)

Full set of changes: [`v0.2.9...v0.2.10`](https://github.com/splitgraph/sgr/compare/v0.2.9...v0.2.10)

## v0.2.9 (2021-03-12)

  * Add a Snowflake data source, backed by a SQLAlchemy connector (https://github.com/splitgraph/sgr/pull/404)

Full set of changes: [`v0.2.8...v0.2.9`](https://github.com/splitgraph/sgr/compare/v0.2.8...v0.2.9)

## v0.2.8 (2021-03-09)

  * Allow deleting tags on remote registries (https://github.com/splitgraph/sgr/pull/403)

Full set of changes: [`v0.2.7...v0.2.8`](https://github.com/splitgraph/sgr/compare/v0.2.7...v0.2.8)

## v0.2.7 (2021-03-09)

  * Fix MySQL plugin crashes on binary data types.

Full set of changes: [`v0.2.6...v0.2.7`](https://github.com/splitgraph/sgr/compare/v0.2.6...v0.2.7)

## v0.2.6 (2021-03-04)

  * Fix querying when there are NULLs in primary keys (https://github.com/splitgraph/sgr/pull/373)
  * Data source and foreign data wrapper for querying CSV files in S3 buckets and HTTP (https://github.com/splitgraph/sgr/pull/397)
  * Ctrl+C can now interrupt long-running PostgreSQL queries and stop sgr (https://github.com/splitgraph/sgr/pull/398)
  * Support for updating miscellaneous repository metadata from the `sgr cloud metadata` CLI (https://github.com/splitgraph/sgr/pull/399)

Full set of changes: [`v0.2.5...v0.2.6`](https://github.com/splitgraph/sgr/compare/v0.2.5...v0.2.6)

## v0.2.5 (2021-01-25)

  * Fix piping CSV files from stdin (https://github.com/splitgraph/sgr/pull/350)
  * Truncate commit comments if they're above the max field size (currently 4096) (https://github.com/splitgraph/sgr/pull/353)
  * Add support for updating repository topics from the CLI (`sgr cloud metadata`) (https://github.com/splitgraph/sgr/pull/371)

Full set of changes: [`v0.2.4...v0.2.5`](https://github.com/splitgraph/sgr/compare/v0.2.4...v0.2.5)

## v0.2.4 (2020-12-08)

  * Mount handlers are now called "data sources", a generalization that will make them more pluggable
    and support sources beyond FDWs. See https://github.com/splitgraph/sgr/pull/324 for more documentation and necessary steps to migrate.
  * Added `sgr singer target`, a Singer-compatible target that can read [Singer tap](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#singer-specification) output from stdin and build Splitgraph images. It's based on a fork of https://github.com/transferwise/pipelinewise-singer-python with additions that let us produce deltas and ingest them directly as Splitgraph objects.
  * Support for dynamically loading plugins without specifying them in `.sgconfig`, by looking up plugins in a certain directory (see https://github.com/splitgraph/sgr/pull/329)

Full set of changes: [`v0.2.3...v0.2.4`](https://github.com/splitgraph/sgr/compare/v0.2.3...v0.2.4)

## v0.2.3 (2020-09-16)

  * Socrata FDW now correctly emits `IS NULL / IS NOT NULL`, same with ES (using ES query syntax).
  * Fix array handling (`a IN (1,2,3)` queries get rewritten and pushed down correctly).
  * Output more query information in `EXPLAIN` for Socrata/LQ.

Full set of changes: [`v0.2.2...v0.2.3`](https://github.com/splitgraph/sgr/compare/v0.2.2...v0.2.3)

## v0.2.2 (2020-09-16)

* Add ability to pass extra server args to postgres_fdw (`extra_server_args`)
* Add ability to rename object files in-engine (utility function for some ingestion).
* Allow disabling `IMPORT FOREIGN SCHEMA` and passing a table schema in Postgres/MySQL FDWs.
* Add a fork (https://github.com/splitgraph/postgres-elasticsearch-fdw) of https://github.com/matthewfranglen/postgres-elasticsearch-fdw to `sgr mount`, letting others mount ES indexes. Fork changes:
  * Pass qualifiers as ElasticSearch queries using the query DSL (was using the `query=...` qual as a Lucene query string, which is useless in JOINs. Now we combine both the query implied from the quals and the Lucene query string, if passed)
  * Close the search context on `end_scan` (otherwise many ES queries to the FDW in a 10 minute span would cause it to error with a "too many scroll contexts" exception)
  * Add EXPLAIN support (outputs the used ES query)

Full set of changes: [`v0.2.1...v0.2.2`](https://github.com/splitgraph/sgr/compare/v0.2.1...v0.2.2)

## v0.2.1 (2020-09-02)

* Add ability to skip config injection at the end of config-manipulating functions (pass `-s`) and don't fail if the Docker socket isn't reachable

Full set of changes: [`v0.2.0...v0.2.1`](https://github.com/splitgraph/sgr/compare/v0.2.0...v0.2.1)

## v0.2.0 (2020-08-18)

* Introducing the [Splitgraph Data Delivery Network](https://www.splitgraph.com/docs/splitgraph-cloud/data-delivery-network): a single SQL endpoint to query all datasets hosted on or proxied by Splitgraph Cloud with any PostgreSQL client.
* Extra `sgr cloud` commands:
  * `sgr cloud sql` to query the Splitgraph DDN
  * `sgr cloud search`, a CLI wrapper around https://www.splitgraph.com/search
* Add daily update check to `sgr`.

Full set of changes: [`v0.1.4...v0.2.0`](https://github.com/splitgraph/sgr/compare/v0.1.4...v0.2.0)

## v0.1.4 (2020-07-19)

* Various dependency bumps (including PostGIS)
* Fix Splitfiles and `sgr import` not respectng the `SG_COMMIT_CHUNK_SIZE` envvar/config variable

Full set of changes: [`v0.1.3...v0.1.4`](https://github.com/splitgraph/sgr/compare/v0.1.3...v0.1.4)

## v0.1.3 (2020-06-27)

* Fix Socrata querying for datasets with long column names (https://github.com/splitgraph/sgr/pull/268)

Full set of changes: [`v0.1.2...v0.1.3`](https://github.com/splitgraph/sgr/compare/v0.1.2...v0.1.3)

## v0.1.2 (2020-06-23)

* Example for writing a custom FDW and integrating it with Splitgraph
* Add dbt adapter that uses Splitgraph data and a sample dbt project
* Socrata UX improvements
* Command line parameters that require JSON now also accept `@filename.json` or `@-` for stdin

Full set of changes: [`v0.1.1...v0.1.2`](https://github.com/splitgraph/sgr/compare/v0.1.1...v0.1.2)

## v0.1.1 (2020-06-12)

* Fixed Socrata querying for datasets with columns that match keywords (e.g. `first`/`last`)

Full set of changes: [`v0.1.0...v0.1.1`](https://github.com/splitgraph/sgr/compare/v0.1.0...v0.1.1)

## v0.1.0 (2020-06-05)

* Initial release.
