# Joining two databases together with dbt

Splitgraph has first-class support for PostgreSQL [foreign data wrappers](https://www.splitgraph.com/docs/ingesting-data/foreign-data-wrappers/introduction),
allowing you to access other databases directly from your Splitgraph instance without having to ETL them into Splitgraph.

This example will show you how to run dbt against a Splitgraph engine to join data between PostgreSQL and MongoDB. You can easily
extend it to run operations across any database for which a foreign data wrapper is available. Check the [PostgreSQL wiki](https://wiki.postgresql.org/wiki/Foreign_data_wrappers) for a list of available PostgreSQL foreign data wrappers. 

## Prerequisites

You need the Splitgraph `sgr` client and dbt installed, as well as Docker and Docker Compose.

See the [Splitgraph documentation](https://www.splitgraph.com/docs/getting-started/installation) for the
installation instructions. If you already have a working Python environment, the simplest way is:

```
$ pip install splitgraph dbt
```

## Usage

The Docker Compose stack contains:

  * a [Splitgraph engine](https://www.splitgraph.com/docs/architecture/splitgraph-engine)
  * a PostgreSQL instance with [some sample data](./splitgraph/postgresql/setup.sql)
  * a MongoDB instance with [more sample data](./splitgraph/mongodb/setup.js)

We will first "mount" the two databases into the Splitgraph engine and then run 
a [dbt model](./models/splitgraph/join_two_dbs.sql). The model will run an SQL JOIN between the 
data in the two databases.

First, start the stack and test that Splitgraph and dbt can connect to the Splitgraph engine.
This example uses a custom [dbt profile](.dbt/profiles.yml). It gives the credentials to connect
to the Splitgraph engine to dbt.

```
$ docker-compose up -d
$ sgr init
$ dbt debug --profiles-dir .dbt
```

Mount the [PostgreSQL database](https://www.splitgraph.com/docs/ingesting-data/foreign-data-wrappers/load-postgres-tables)
into your Splitgraph engine. This will create a schema `fruits_data`
with a single table, `fruits`. When an SQL client queries this table, the foreign data wrapper
will forward the query to the remote database and give the results back to the client.

```
$ sgr mount postgres_fdw fruits_data -c originuser:originpass@postgres:5432 -o @- <<EOF
{
    "dbname": "origindb",
    "remote_schema": "public"
}
EOF
```

Test that the data is now available on the engine. You can do this with any PostgreSQL client, but
Splitgraph offers a shorthand to run SQL statements against the engine:

```
$ sgr sql "SELECT * FROM fruits_data.fruits"

1  apple
2  orange
3  tomato
```

Do the same thing with MongoDB. Because MongoDB is schemaless, we have to specify the schema
that we wish our foreign table to have. Read the [Splitgraph MongoDB documentation](https://www.splitgraph.com/docs/ingesting-data/foreign-data-wrappers/load-mongo-collections) for more information.

```
$ sgr mount mongo_fdw order_data -c originro:originpass@mongo:27017 -o @- <<EOF
{
    "orders":
    {
        "database": "origindb",
        "collection": "orders",
        "schema":
        {
            "name": "text",
            "fruit_id": "numeric",
            "happy": "boolean",
            "review": "text"
        }
    }
}
EOF
```

Check we can get data from MongoDB too:

```
$ sgr sql "SELECT * FROM order_data.orders"

5f0c736093455fe435231159  Alex   1  False  Was in transit for five days, arrived rotten.
5f0c736093455fe43523115a  James  2  True
5f0c736093455fe43523115b  Alice  3  True   Will use in salad, great fruit!
```

Note that `mongo_fdw` adds the Mongo object ID column as the first column of all tables. This will not
be a problem for us.

The [dbt model](./models/splitgraph/join_two_dbs.sql) consists of a simple JOIN statement:

```sql
with fruits as (
    select fruit_id, name from fruits_data.fruits
),
orders as (
    select name, fruit_id, happy, review
    from order_data.orders
)

select fruits.name as fruit, orders.name as customer, review
from fruits join orders
on fruits.fruit_id = orders.fruit_id
```

Run it:

```
$ dbt run --profiles-dir .dbt

Running with dbt=0.17.0
Found 1 model, 0 tests, 0 snapshots, 0 analyses, 134 macros, 0 operations, 0 seed files, 0 sources

15:52:44 | Concurrency: 4 threads (target='splitgraph')
15:52:44 | 
15:52:44 | 1 of 1 START table model dbt_two_databases.join_two_dbs.............. [RUN]
15:52:44 | 1 of 1 OK created table model dbt_two_databases.join_two_dbs......... [SELECT 3 in 0.33s]
15:52:44 | 
15:52:44 | Finished running 1 table model in 0.47s.

Completed successfully

Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1
```

Check the dataset that dbt built:

```
$ sgr sql "SELECT fruit, customer, review FROM dbt_two_databases.join_two_dbs"

apple   Alex   Was in transit for five days, arrived rotten.
orange  James
tomato  Alice  Will use in salad, great fruit!
```

## Further reading

  * [Splitgraph foreign data wrapper documentation](https://www.splitgraph.com/docs/ingesting-data/foreign-data-wrappers/introduction)
  * [List of PostgreSQL foreign data wrappers](https://wiki.postgresql.org/wiki/Foreign_data_wrappers)
  * [Splitgraph-dbt integration documentation](https://www.splitgraph.com/docs/integrating-splitgraph/dbt)
  * ["Foreign data wrappers: PostgreSQL's secret weapon?"](https://www.splitgraph.com/blog/foreign-data-wrappers): a blog post that talks about writing a custom foreign data wrapper and using it with Splitgraph.
