# Running PostgREST against the Splitgraph engine

Since a Splitgraph engine is also a PostgreSQL database, tools that use PostgreSQL can work
with Splitgraph tables without any changes.

One such tool is [PostgREST](http://postgrest.org/en/latest/) that generates a REST API for a
PostgreSQL schema. Splitgraph runs PostgREST in Splitgraph Cloud, allowing any Splitgraph dataset
to be accessed via HTTP. For example, [this](https://data.splitgraph.com/splitgraph/domestic_us_flights/latest/-/rest/flights?and=(origin_airport.eq.JFK,destination_airport.eq.LAX)) link runs the following PostgREST query against
the [`splitgraph/domestic_us_flights:latest`](https://www.splitgraph.com/splitgraph/domestic_us_flights/latest/-/overview) image:

```
flights?and=(origin_airport.eq.JFK,destination_airport.eq.LAX)
```

You can reproduce a similar setup locally, getting PostgREST to work against a Splitgraph image.

This example will:

* Set up a Splitgraph engine with some sample data
* Run a PostgREST instance against the engine
* Use curl to query the PostgREST instance.
* Swap the schema to be a layered checkout, which still looks like a regular schema
  to PostgREST but has the ability to lazily download and cache required fragments
  of the dataset on the fly. 

## Running the example

`../run_example.py example.yaml` and press ENTER when prompted to go through the steps.
