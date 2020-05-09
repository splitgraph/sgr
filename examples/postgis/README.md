# Handling geospatial data images in Splitgraph: plotting voting precincts

Splitgraph can also work with [PostGIS](https://postgis.net/), an extension for PostgreSQL that
adds support for geospatial data. By default, the Splitgraph engine doesn't come with PostGIS
installed, but you can use a PostGIS-enabled engine Docker image or build your own.

This example will:

  * Use a Splitfile to build a dataset containing [precinct boundaries](https://www.splitgraph.com/splitgraph/election-geodata/) and Trump/Clinton
    vote fractions in each precinct in New York City
  * Use [geopandas](https://geopandas.org) to plot this data on a map.
  
**Data sources**:
  * [Precinct-level maps](https://www.splitgraph.com/splitgraph/election-geodata/) from [election-geodata](https://github.com/nvkelso/election-geodata) compiled by Nathaniel Kelso and Michal Migurski.
  * [2016 US Presidential Election precinct-level returns](https://www.splitgraph.com/splitgraph/2016_election/) ([source](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/LYWX3D))
  
You can view the [notebook](./vote_map.ipynb) directly or run it yourself.

## Running the example

First, you need to have a PostGIS-enabled Splitgraph engine running. 

### `docker-compose`

You can set up an engine using `docker-compose`:

```
export COMPOSE_PROJECT_NAME=splitgraph_example 
docker-compose down -v
docker-compose build
docker-compose up -d
sgr init
```

Then, copy your .sgconfig file into this directory (it must contain API credentials to access
data.splitgraph.com). If you don't have them yet, take a look at the
[getting started example](../get-started/README.md) or register using `sgr cloud register`.

### `sgr engine` CLI

If you're using the `sgr engine` wrapper, you can upgrade your engine to use PostGIS:

```
sgr engine upgrade --image splitgraph/engine:[your_engine_version]-postgis 
```

Make sure that you've logged into Splitgraph Cloud with `sgr cloud login` or `sgr cloud login-api`.

### Setting up and running the Jupyter notebook

Install this package with [Poetry](https://github.com/sdispater/poetry): `poetry install`.

Open the notebook in Jupyter: `jupyter notebook vote_map.ipynb`.
