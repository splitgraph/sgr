# `sgr`

![Build status](https://github.com/splitgraph/sgr/workflows/build_all/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/splitgraph/splitgraph/badge.svg?branch=master)](https://coveralls.io/github/splitgraph/splitgraph?branch=master)
[![PyPI version](https://badge.fury.io/py/splitgraph.svg)](https://badge.fury.io/py/splitgraph)
[![Discord chat room](https://img.shields.io/discord/718534846472912936.svg)](https://discord.gg/4Qe2fYA)
[![Follow](https://img.shields.io/badge/twitter-@Splitgraph-blue.svg)](https://twitter.com/Splitgraph)

## Overview

**`sgr`** is the CLI for [**Splitgraph**](https://www.splitgraph.com), a
serverless API for data-driven Web applications.

With addition of the optional [`sgr` Engine](engine/README.md) component, `sgr`
can become a stand-alone tool for building, versioning and querying reproducible
datasets. We use it as the storage engine for Splitgraph. It's inspired by
Docker and Git, so it feels familiar. And it's powered by
[PostgreSQL](https://postgresql.org), so it works seamlessly with existing tools
in the Postgres ecosystem. Use `sgr` to package your data into self-contained
**Splitgraph data images** that you can
[share with other `sgr` instances](https://www.splitgraph.com/docs/getting-started/decentralized-demo).

To install the `sgr` CLI or a local `sgr` Engine, see the
[Installation](#installation) section of this readme.

### Build and Query Versioned, Reproducible Datasets

[**Splitfiles**](https://www.splitgraph.com/docs/concepts/splitfiles) give you a
declarative language, inspired by Dockerfiles, for expressing data
transformations in ordinary SQL familiar to any researcher or business analyst.
You can reference other images, or even other databases, with a simple JOIN.

![](pics/splitfile.png)

When you build data images with Splitfiles, you get provenance tracking of the
resulting data: it's possible to find out what sources went into every dataset
and know when to rebuild it if the sources ever change. You can easily integrate
`sgr` your existing CI pipelines, to keep your data up-to-date and stay on top
of changes to upstream sources.

Splitgraph images are also version-controlled, and you can manipulate them with
Git-like operations through a CLI. You can check out any image into a PostgreSQL
schema and interact with it using any PostgreSQL client. `sgr` will capture your
changes to the data, and then you can commit them as delta-compressed changesets
that you can package into new images.

`sgr` supports PostgreSQL
[foreign data wrappers](https://wiki.postgresql.org/wiki/Foreign_data_wrappers).
We call this feature
[mounting](https://www.splitgraph.com/docs/concepts/mounting). With mounting,
you can query other databases (like PostgreSQL/MongoDB/MySQL) or open data
providers (like
[Socrata](https://www.splitgraph.com/docs/ingesting-data/socrata)) from your
`sgr` instance with plain SQL. You can even snapshot the results or use them in
Splitfiles.

![](pics/splitfiles.gif)

## Components

The code in this repository contains:

- **[`sgr` CLI](https://www.splitgraph.com/docs/architecture/sgr-client)**:
  `sgr` is the main command line tool used to work with Splitgraph "images"
  (data snapshots). Use it to ingest data, work with Splitfiles, and push data
  to Splitgraph.
- **[`sgr` Engine](https://github.com/splitgraph/sgr/blob/master/engine/README.md)**: a
  [Docker image](https://hub.docker.com/r/splitgraph/engine) of the latest
  Postgres with `sgr` and other required extensions pre-installed.
- **[Splitgraph Python library](https://www.splitgraph.com/docs/python-api/splitgraph.core)**:
  All `sgr` functionality is available in the Python API, offering first-class
  support for data science workflows including Jupyter notebooks and Pandas
  dataframes.

## Docs

- [`sgr` documentation](https://www.splitgraph.com/docs/sgr-cli/introduction)
- [Advanced `sgr` documentation](https://www.splitgraph.com/docs/sgr-advanced/getting-started/introduction)
- [`sgr` command reference](https://www.splitgraph.com/docs/sgr/image-management-creation/checkout_)
- [`splitgraph` package reference](https://www.splitgraph.com/docs/python-api/modules)

We also recommend reading our Blog, including some of our favorite posts:

- [Supercharging `dbt` with `sgr`: versioning, sharing, cross-DB joins](https://www.splitgraph.com/blog/dbt)
- [Querying 40,000+ datasets with SQL](https://www.splitgraph.com/blog/40k-sql-datasets)
- [Foreign data wrappers: PostgreSQL's secret weapon?](https://www.splitgraph.com/blog/foreign-data-wrappers)

## Installation

Pre-requisites:

- Docker is required to run the `sgr` Engine. `sgr` must have access to Docker.
  You either need to [install Docker locally](https://docs.docker.com/install/)
  or have access to a remote Docker socket.

You can get the `sgr` single binary from
[the releases page](https://github.com/splitgraph/sgr/releases).
Optionally, you can run
[`sgr engine add`](https://www.splitgraph.com/docs/sgr/engine-management/engine-add)
to create an engine.

For Linux and OSX, once Docker is running, install `sgr` with a single script:

```bash
$ bash -c "$(curl -sL https://github.com/splitgraph/sgr/releases/latest/download/install.sh)"
```

This will download the `sgr` binary and set up the `sgr` Engine Docker
container.

See the
[installation guide](https://www.splitgraph.com/docs/sgr-cli/installation) for
more installation methods.

## Quick start guide

You can follow the
[quick start guide](https://www.splitgraph.com/docs/sgr-advanced/getting-started/five-minute-demo)
that will guide you through the basics of using `sgr` with Splitgraph or
standalone.

Alternatively, `sgr` comes with plenty of [examples](https://github.com/splitgraph/sgr/tree/master/examples) to get you
started.

If you're stuck or have any questions, check out the
[documentation](https://www.splitgraph.com/docs/sgr-advanced/getting-started/introduction)
or join our [Discord channel](https://discord.gg/4Qe2fYA)!

## Contributing

### Setting up a development environment

- `sgr` requires Python 3.7 or later.
- Install [Poetry](https://github.com/python-poetry/poetry):
  `curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python`
  to manage dependencies
- Install pre-commit hooks (we use [Black](https://github.com/psf/black) to
  format code)
- `git clone --recurse-submodules https://github.com/splitgraph/sgr.git`
- `poetry install`
- To build the
  [engine](https://www.splitgraph.com/docs/architecture/splitgraph-engine)
  Docker image: `cd engine && make`

### Running tests

The test suite requires [docker-compose](https://github.com/docker/compose). You
will also need to add these lines to your `/etc/hosts` or equivalent:

```
127.0.0.1       local_engine
127.0.0.1       remote_engine
127.0.0.1       objectstorage
```

To run the core test suite, do

```
docker-compose -f test/architecture/docker-compose.core.yml up -d
poetry run pytest -m "not mounting and not example"
```

To run the test suite related to "mounting" and importing data from other
databases (PostgreSQL, MySQL, Mongo), do

```
docker-compose -f test/architecture/docker-compose.core.yml -f test/architecture/docker-compose.mounting.yml up -d
poetry run pytest -m mounting
```

Finally, to test the
[example projects](https://github.com/splitgraph/sgr/tree/master/examples),
do

```
# Example projects spin up their own engines
docker-compose -f test/architecture/docker-compose.core.yml -f test/architecture/docker-compose.core.yml down -v
poetry run pytest -m example
```

All of these tests run in
[CI](https://github.com/splitgraph/sgr/actions).
