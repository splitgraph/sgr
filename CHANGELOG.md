# Changelog

## v0.2.3 (2020-09-16)

  * Socrata FDW now correctly emits `IS NULL / IS NOT NULL`, same with ES (using ES query syntax).
  * Fix array handling (`a IN (1,2,3)` queries get rewritten and pushed down correctly).
  * Output more query information in `EXPLAIN` for Socrata/LQ.

Full set of changes: [`v0.2.2...v0.2.3`](https://github.com/splitgraph/splitgraph/compare/v0.2.2...v0.2.3)

## v0.2.2 (2020-09-16)

* Add ability to pass extra server args to postgres_fdw (`extra_server_args`)
* Add ability to rename object files in-engine (utility function for some ingestion).
* Allow disabling `IMPORT FOREIGN SCHEMA` and passing a table schema in Postgres/MySQL FDWs.
* Add a fork (https://github.com/splitgraph/postgres-elasticsearch-fdw) of https://github.com/matthewfranglen/postgres-elasticsearch-fdw to `sgr mount`, letting others mount ES indexes. Fork changes:
  * Pass qualifiers as ElasticSearch queries using the query DSL (was using the `query=...` qual as a Lucene query string, which is useless in JOINs. Now we combine both the query implied from the quals and the Lucene query string, if passed)
  * Close the search context on `end_scan` (otherwise many ES queries to the FDW in a 10 minute span would cause it to error with a "too many scroll contexts" exception)
  * Add EXPLAIN support (outputs the used ES query)

Full set of changes: [`v0.2.1...v0.2.2`](https://github.com/splitgraph/splitgraph/compare/v0.2.1...v0.2.2)

## v0.2.1 (2020-09-02)

* Add ability to skip config injection at the end of config-manipulating functions (pass `-s`) and don't fail if the Docker socket isn't reachable

Full set of changes: [`v0.2.0...v0.2.1`](https://github.com/splitgraph/splitgraph/compare/v0.2.0...v0.2.1)

## v0.2.0 (2020-08-18)

* Introducing the [Splitgraph Data Delivery Network](https://www.splitgraph.com/docs/splitgraph-cloud/data-delivery-network): a single SQL endpoint to query all datasets hosted on or proxied by Splitgraph Cloud with any PostgreSQL client.
* Extra `sgr cloud` commands:
  * `sgr cloud sql` to query the Splitgraph DDN
  * `sgr cloud search`, a CLI wrapper around https://www.splitgraph.com/search
* Add daily update check to `sgr`.
 
Full set of changes: [`v0.1.4...v0.2.0`](https://github.com/splitgraph/splitgraph/compare/v0.1.4...v0.2.0)

## v0.1.4 (2020-07-19)

* Various dependency bumps (including PostGIS)
* Fix Splitfiles and `sgr import` not respectng the `SG_COMMIT_CHUNK_SIZE` envvar/config variable

Full set of changes: [`v0.1.3...v0.1.4`](https://github.com/splitgraph/splitgraph/compare/v0.1.3...v0.1.4)

## v0.1.3 (2020-06-27)

* Fix Socrata querying for datasets with long column names (https://github.com/splitgraph/splitgraph/pull/268)

Full set of changes: [`v0.1.2...v0.1.3`](https://github.com/splitgraph/splitgraph/compare/v0.1.2...v0.1.3)

## v0.1.2 (2020-06-23)

* Example for writing a custom FDW and integrating it with Splitgraph
* Add dbt adapter that uses Splitgraph data and a sample dbt project
* Socrata UX improvements
* Command line parameters that require JSON now also accept `@filename.json` or `@-` for stdin

Full set of changes: [`v0.1.1...v0.1.2`](https://github.com/splitgraph/splitgraph/compare/v0.1.1...v0.1.2)

## v0.1.1 (2020-06-12)

* Fixed Socrata querying for datasets with columns that match keywords (e.g. `first`/`last`)

Full set of changes: [`v0.1.0...v0.1.1`](https://github.com/splitgraph/splitgraph/compare/v0.1.0...v0.1.1)

## v0.1.0 (2020-06-05)

* Initial release.
