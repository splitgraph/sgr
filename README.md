[![Splitgraph](https://splitgraph.com/img/logo-colour-full.svg)](https://splitgraph.com)

![](https://www.mildbyte.xyz/asciicast/splitfiles.gif)

# Splitgraph
![Build status](https://github.com/splitgraph/splitgraph/workflows/build_all/badge.svg) [![Coverage Status](https://coveralls.io/repos/github/splitgraph/splitgraph/badge.svg?branch=master)](https://coveralls.io/github/splitgraph/splitgraph?branch=master)

Splitgraph is a tool for building, versioning, querying and sharing datasets that works on top of [PostgreSQL](https://postgresql.org) and integrates seamlessly with anything that uses PostgreSQL.

This repository contains most of the core code for the Splitgraph library, 
the [`sgr` command line client](https://www.splitgraph.com/docs/architecture/sgr_client) and the [Splitgraph Engine](engine/README.md). 

See https://splitgraph.com/docs/introduction.html for the full docs.

## Installation

You will need access to [Docker](https://docs.docker.com/install/) as Splitgraph uses it to run
the Splitgraph Engine.

For Linux and OSX, there's a single script:

```
$ bash -c "$(curl -sL https://github.com/splitgraph/splitgraph/releases/latest/download/install.sh)"
```

This script downloads the `sgr` binary and sets up the Splitgraph Engine Docker container.

Alternatively, you can get the `sgr` single binary from [the releases page](https://github.com/splitgraph/splitgraph/releases) and run [`sgr engine add`](https://www.splitgraph.com/docs/sgr/engine_management/engine_add) to create an engine.

See the [installation guide](https://www.splitgraph.com/docs/getting-started/installation) for more installation methods.

## Quick start guide

You can follow the [quick start guide](https://www.splitgraph.com/docs/getting-started/five_minute_demo) to try cloning, checking out and manipulating a data repository.

Alternatively, Splitgraph comes with plenty of [examples](examples)
to get you started. 

## Setting up a development environment

  * Splitgraph requires Python 3.6 or later.
  * Install [Poetry](https://github.com/python-poetry/poetry): `curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python` to manage dependencies
  * Install pre-commit hooks (we use [Black](https://github.com/psf/black) to format code)
  * `git clone https://github.com/splitgraph/splitgraph.git`
  * `poetry install` 
  * To build the [engine](https://www.splitgraph.com/docs/architecture/splitgraph_engine) Docker image: `cd engine && make`

### Running tests

The test suite requires [docker-compose](https://github.com/docker/compose). You will also
need to add these lines to your `/etc/hosts` or equivalent:

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

To run the test suite related to "mounting" and importing data from  other databases
(PostgreSQL, MySQL, Mongo), do

```
docker-compose -f test/architecture/docker-compose.core.yml -f test/architecture/docker-compose.core.yml up -d  
poetry run pytest -m mounting
```

Finally, to test the [example projects](https://github.com/splitgraph/splitgraph/tree/master/examples), do

```
# Example projects spin up their own engines
docker-compose -f test/architecture/docker-compose.core.yml -f test/architecture/docker-compose.core.yml down -v
poetry run pytest -m example
```

All of these tests run in [CI](https://github.com/splitgraph/splitgraph/actions).
