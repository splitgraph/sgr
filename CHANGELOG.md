# Changelog

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
