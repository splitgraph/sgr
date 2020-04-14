# Splitgraph benchmarking

Since Splitgraph defers to Postgres for a lot of its operations (e.g. after checkout, a Splitgraph
table becomes a normal PostgreSQL table with change tracking enabled), it's difficult to specify
what is considered a benchmark for Splitgraph.

There are two Jupyter notebooks here. The first one, [benchmarking](./benchmarking.ipynb), 
tests the overhead of common Splitgraph operations on a series of synthetic PostgreSQL tables
and compares dataset sizes when stored in Splitgraph vs when stored as PostgreSQL tables.  

The second one, [benchmarking_real_data](./benchmarking_real_data.ipynb), uses some datasets
that are available on the Splitgraph registry to compare the size improvement from storing
them as Splitgraph objects as well as benchmarks querying Splitgraph repositories directly
(using layered querying) vs querying them as PostgreSQL tables. 

## Running the example

You can view the notebooks in your browser. Alternatively, you can build and start up the engine:

```
export COMPOSE_PROJECT_NAME=splitgraph_example 
docker-compose down -v
docker-compose build
docker-compose up -d
sgr init
```

You need to have been logged into the registry (`sgr cloud login`).

You can also use your own engine that's managed by `sgr engine`.

Install this package with [Poetry](https://github.com/sdispater/poetry): `poetry install`

Open the notebook in Jupyter: `jupyter notebook benchmarking.ipynb` or `jupyter notebook benchmarking_real_data.ipynb`
