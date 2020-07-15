# Using Splitgraph data in ClickHouse

This example will show you a workflow for connecting to Splitgraph through ODBC, letting you query
Splitgraph data from anything that supports ODBC. This includes datasets hosted by Splitgraph as well as
ones hosted by third party vendors (for example, [Socrata](https://www.splitgraph.com/docs/ingesting-data/socrata)).

In this case, we will be using [ClickHouse](https://clickhouse.tech), a fast open-source column-oriented database management system,
to query Splitgraph data through ODBC.

This uses [unixODBC](http://www.unixodbc.org/) and ClickHouse's [ODBC functionality](https://clickhouse.tech/docs/en/engines/table-engines/integrations/odbc/).

## Prerequisites

You will need Docker and Docker Compose. You will also need Splitgraph.
See the [Splitgraph documentation](https://www.splitgraph.com/docs/getting-started/installation) for the installation instructions.

## Guide

The Docker Compose stack contains:

  * The Splitgraph engine
  * An instance of the ClickHouse server with unixODBC installed
  * ClickHouse client
  
Here is an architecture diagram of this setup:

![[](./splitgraph-odbc-clickhouse.png)](./splitgraph-odbc-clickhouse.png)

### Setup

Start the stack. This will build ClickHouse with ODBC configured and start the Splitgraph engine.

```shell-session
$ docker-compose up -d --build
```

Initialize the engine and set up some datasets:

  * Chicago Fire Stations Socrata dataset in `chicago_data.fire_stations`
  * Foreign tables for the full Chicago Open Data Socrata domain in `chicago`
  * Foreign tables for the full Cambridge Open Data Socrata domain in `cambridge`

```bash
$ ./setup_datasets.sh

Initializing engine PostgresEngine LOCAL (sgr@localhost:5432/splitgraph)...
Database splitgraph already exists, skipping
Ensuring the metadata schema at splitgraph_meta exists...
Running splitgraph_meta--0.0.1.sql
Running splitgraph_meta--0.0.1--0.0.2.sql
Running splitgraph_meta--0.0.2--0.0.3.sql
Installing Splitgraph API functions...
Installing CStore management functions...
Installing the audit trigger...
Engine PostgresEngine LOCAL (sgr@localhost:5432/splitgraph) initialized.

Mounting Socrata domain...
Getting Socrata metadata
warning: Requests made without an app_token will be subject to strict throttling limits.
Loaded metadata for 1 Socrata table

Mounting Socrata domain...
Getting Socrata metadata
Loaded metadata for 515 Socrata tables

Mounting Socrata domain...
Getting Socrata metadata
warning: Requests made without an app_token will be subject to strict throttling limits.
Loaded metadata for 137 Socrata tables
```

When anything queries these tables, Splitgraph will rewrite queries to SoQL (Socrata Query Language)
and forward them to Socrata's servers.

### Basic ODBC operations

You can now start the ClickHouse client and run a simple query that references Splitgraph through ODBC:

```bash
$ docker-compose run --rm clickhouse-client

ClickHouse client version 20.5.2.7 (official build).
Connecting to clickhouse-server:9000 as user default.
Connected to ClickHouse server version 20.5.2 revision 54435.


538c1b6dd221 :) SELECT engine, address FROM odbc('DSN=splitgraph', 'chicago_data', 'fire_stations') LIMIT 10;

SELECT 
    engine,
    address
FROM odbc('DSN=splitgraph', 'chicago_data', 'fire_stations')
LIMIT 10

┌─engine─┬─address───────────────┐
│ E73    │ 8630 S EMERALD AVE    │
│ E4     │ 548 W DIVISION ST     │
│ E64    │ 7659 S PULASKI RD     │
│ E43    │ 2179 N STAVE ST       │
│ E70/59 │ 6030 N CLARK ST       │
│ E71    │ 6239 N CALIFORNIA AVE │
│ E124   │ 4426 N KEDZIE AVE     │
│ E69    │ 4017 N TRIPP AVE      │
│ E22    │ 605 W ARMITAGE AVE    │
│ E103   │ 25 S LAFLIN ST        │
└────────┴───────────────────────┘

10 rows in set. Elapsed: 0.304 sec. 
```

### Layered querying

For basic operations, just using the `odbc()` wrapper is sufficient. This will also forward qualifiers
(filters) to Splitgraph. In addition to forwarding qualifiers to Socrata, this lets you use
[layered querying](https://www.splitgraph.com/docs/large-datasets/layered-querying)
to query remote datasets hosted on Splitgraph by only downloading required objects.

For example, let's clone the metadata for the [2016 US Presidential Election](https://www.splitgraph.com/splitgraph/2016_election) dataset and query it.

You need to log in to the Splitgraph registry to run this (`sgr cloud login`):

```
$ sgr clone splitgraph/2016_election:latest
Gathering remote metadata...
Fetched metadata for 1 image, 1 table, 20 objects and 1 tag.

$ sgr checkout --layered splitgraph/2016_election:latest
Checked out splitgraph/2016_election:3835145ada3f.
```

Go back to the ClickHouse client and query the data:

```
538c1b6dd221 :) SELECT candidate_normalized, SUM(votes) AS votes FROM odbc('DSN=splitgraph', 'splitgraph/2016_election', 'precinct_results') WHERE state_postal = 'TX' GROUP BY candidate_normalized ORDER BY votes DESC;

SELECT 
    candidate_normalized,
    SUM(votes) AS votes
FROM odbc('DSN=splitgraph', 'splitgraph/2016_election', 'precinct_results')
WHERE state_postal = 'TX'
GROUP BY candidate_normalized
ORDER BY votes DESC

┌─candidate_normalized─┬───votes─┐
│ trump                │ 4684288 │
│ clinton              │ 3877626 │
│ johnson              │  283462 │
│ stein                │   71546 │
│ mcmullin             │   42366 │
│ castle               │    4261 │
│ maturen              │    1401 │
│ kotlikoff            │    1037 │
│ hoefling             │     932 │
│ valdivia             │     428 │
│ cubbler              │     314 │
│ morrow               │     145 │
│ moorehead            │     122 │
│ soltysik             │      72 │
│ steffes              │      71 │
│ lee                  │      67 │
│ fox                  │      45 │
└──────────────────────┴─────────┘

17 rows in set. Elapsed: 0.639 sec. Processed 39.07 thousand rows, 1.45 MB (61.10 thousand rows/s., 2.27 MB/s.) 
```

By running `sgr status`, you can see that this query only downloaded 2.64MiB of data out of 26.81MiB:

```shell-session
$ sgr status

Local repositories: 

Repository                  Images    Tags  Size (T)    Size (A)    Checkout         Upstream
------------------------  --------  ------  ----------  ----------  ---------------  ----------------------------------------------
splitgraph/2016_election         1       1  26.81 MiB   2.64 MiB    3835145ada (LQ)  splitgraph/2016_election (data.splitgraph.com)
```

This is because ClickHouse pushed the `state_postal = 'TX'` filter through the ODBC connection to Splitgraph,
so Splitgraph could use its own metadata to limit the number of objects it needed to download.

### Advanced ODBC operations: remapping types and joining between multiple datasets

When you need to alter the table definitions that the ODBC adapter uses, you can create a table
on ClickHouse that uses the ODBC engine. In case of Splitgraph, this can be useful for timeseries
data, as it can return it in a format not parseable by ClickHouse.

In this example, we'll reproduce the [Socrata Metabase](https://www.splitgraph.com/docs/ingesting-data/socrata#using-metabase-to-join-and-plot-data-from-multiple-data-portals)
example that joins data between the Chicago and the Cambridge Socrata data portals, plotting the number
of coronavirus cases in both cities.

We had already set up the `chicago` and `cambridge` schemata in the previous section. Go back to
the ClickHouse client and create the ODBC tables:

```sql
CREATE TABLE chicago_cases (
	lab_report_date String,
	cases_total Int32	
)
ENGINE = ODBC('DSN=splitgraph', 'chicago', 'covid19_daily_cases_and_deaths_naz8_j4nc');

CREATE TABLE cambridge_cases (
	date String,
	new_positive_cases Int32
)
ENGINE = ODBC('DSN=splitgraph', 'cambridge', 'covid19_cumulative_cases_by_date_tdt9_vq5y');
```

Here, we're only interested in the date and number of cases columns. In addition, we treat them
as `String` and will parse them ourselves in ClickHouse.

You can now run JOIN queries between these tables as normal:

```
538c1b6dd221 :) SELECT
:-]     parseDateTimeBestEffortOrNull(cambridge_cases.date) AS date,
:-]     chicago_cases.cases_total AS chicago_daily_cases,
:-]     cambridge_cases.new_positive_cases AS cambridge_daily_cases
:-] FROM
:-]     chicago_cases
:-] FULL OUTER JOIN
:-]     cambridge_cases
:-] ON
:-]     parseDateTimeBestEffortOrNull(chicago_cases.lab_report_date) = parseDateTimeBestEffortOrNull(cambridge_cases.date)
:-] ORDER BY date DESC LIMIT 10;

SELECT 
    parseDateTimeBestEffortOrNull(cambridge_cases.date) AS date,
    chicago_cases.cases_total AS chicago_daily_cases,
    cambridge_cases.new_positive_cases AS cambridge_daily_cases
FROM chicago_cases
FULL OUTER JOIN cambridge_cases ON parseDateTimeBestEffortOrNull(chicago_cases.lab_report_date) = parseDateTimeBestEffortOrNull(cambridge_cases.date)
ORDER BY date DESC
LIMIT 10

┌────────────────date─┬─chicago_daily_cases─┬─cambridge_daily_cases─┐
│ 2020-07-13 00:00:00 │                   3 │                     0 │
│ 2020-07-12 00:00:00 │                  23 │                     1 │
│ 2020-07-11 00:00:00 │                  82 │                     2 │
│ 2020-07-10 00:00:00 │                 158 │                     4 │
│ 2020-07-09 00:00:00 │                 207 │                     3 │
│ 2020-07-08 00:00:00 │                 250 │                     0 │
│ 2020-07-07 00:00:00 │                 305 │                     2 │
│ 2020-07-06 00:00:00 │                 250 │                     2 │
│ 2020-07-05 00:00:00 │                  75 │                     1 │
│ 2020-07-04 00:00:00 │                  60 │                     0 │
└─────────────────────┴─────────────────────┴───────────────────────┘

10 rows in set. Elapsed: 0.425 sec. 
```

We use the [`parseTimeBestEffortOrNull`](https://clickhouse.tech/docs/en/sql-reference/functions/type-conversion-functions/#parsedatetimebesteffortornull) function here
to let ClickHouse parse date formats with slashes that Socrata uses.

## Further reading

  * [ClickHouse ODBC documentation](https://clickhouse.tech/docs/en/engines/table-engines/integrations/odbc/)
  * [unixODBC](http://www.unixodbc.org/)
  * [Splitgraph Socrata documentation](https://www.splitgraph.com/docs/ingesting-data/socrata)
  * [Splitgraph layered querying](https://www.splitgraph.com/docs/large-datasets/layered-querying)
