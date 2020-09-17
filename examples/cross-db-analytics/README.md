# Analytics cross-database join: a primer

This is a sample stack that includes Splitgraph and `sgr mount` commands required to create
foreign tables that proxy to Matomo and Elasticsearch. This lets you query Matomo and ES data
directly from Splitgraph.

## Matomo

[`matomo.json`](./mounting/matomo.json) contains a schema spec for the created foreign tables. You can use the [Matomo schema reference](https://developer.matomo.org/guides/database-schema) if there are some tables that are missing here.

Behind the scenes, this uses [mysql_fdw](https://github.com/EnterpriseDB/mysql_fdw).

[`matomo.sql`](./mounting/matomo.sql) builds a sample user-friendly view on top of Matomo data.

## Elasticsearch

You will need to change [`elasticsearch.json`](./mounting/elasticsearch.json) to have the schema matching your Elasticsearch indexes and field names. The FDW in use is a [Splitgraph fork of `pg_es_fdw`](https://github.com/splitgraph/postgres-elasticsearch-fdw) ([original by matthewfranglen](https://github.com/matthewfranglen/postgres-elasticsearch-fdw)) that pushes down PG qualifiers to the Elasticsearch cluster.

[`elasticsearch.sql`](./mounting/elasticsearch.sql) has some sample views that are built based on this mounted data. If a view isn't materialized, this forwards queries to ES.

## Usage

Add your Matomo/Elasticsearch instance to the stack (or run it in network=host mode) and change
the environment variables to point to your credentials.

Change the Elasticsearch schema spec / views if needed as per the previous section.

Then, start the engine and mount the data:

```
docker-compose up -d splitgraph
docker-compose run --rm splitgraph /mounting/mount.sh
```

If, instead, you have Splitgraph installed already, you can start a Splitgraph engine and run the command
directly on the host:

```
sgr engine add
./mounting/mount.sh
```

You can now query these tables as normal over a PostgreSQL protocol:

```
psql postgresql://sgr:supersecure@localhost:5432/splitgraph "SELECT * FROM matomo.visit LIMIT 10"
```
