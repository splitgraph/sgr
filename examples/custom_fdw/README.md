# Writing a custom foreign data wrapper and mount handler for Splitgraph

This example shows how to write a custom [foreign data wrapper](https://www.splitgraph.com/docs/ingesting-data/foreign-data-wrappers/introduction)
using [Multicorn](https://github.com/Segfault-Inc/Multicorn) and then how to
integrate it with Splitgraph's `sgr mount` command, making it available to be queried
on the Splitgraph engine and used in Splitfiles.

In particular, we will be installing a foreign data wrapper that fetches the top, best, new
stories from Hacker News as well as Show HNs and Ask HNs, using the [Firebase API](https://github.com/HackerNews/API).

## Requirements

You need to have Docker and Docker Compose installed.

## Running the example

The Compose stack consists of a custom version of the Splitgraph engine (with the FDW installed)
and a small Python container with the `sgr` client.

The foreign data wrapper that is installed on the engine lives in [./src/hn_fdw/fdw.py](./src/hn_fdw/fdw.py)
and the mount handler that exposes it to Splitgraph is in [./src/hn_fdw/mount.py](./src/hn_fdw/mount.py).

Start the shell with sgr installed and initialize the engine. This will also build and start the
engine container:

```
$ docker-compose run --rm sgr

$ sgr init
Initializing engine PostgresEngine LOCAL (sgr@engine:5432/splitgraph)...
Database splitgraph already exists, skipping
Ensuring the metadata schema at splitgraph_meta exists...
Running splitgraph_meta--0.0.1.sql
Running splitgraph_meta--0.0.1--0.0.2.sql
Running splitgraph_meta--0.0.2--0.0.3.sql
Installing Splitgraph API functions...
Installing CStore management functions...
Installing the audit trigger...
Engine PostgresEngine LOCAL (sgr@engine:5432/splitgraph) initialized.
```

Check the help for the HN mount handler:

```
$ sgr mount hackernews --help
Usage: sgr mount hackernews [OPTIONS] SCHEMA

      Mount a Hacker News story dataset using the Firebase API.

Options:
  -c, --connection TEXT       Connection string in the form
                              username:password@server:port

  -o, --handler-options TEXT  JSON-encoded dictionary of handler options:
                              
                              endpoints: List of Firebase endpoints to mount,
                              mounted into the same tables as     the endpoint
                              name. Supported endpoints:
                              {top,new,best,ask,show,job}stories.

  --help                      Show this message and exit.
```

Now, actually "mount" the dataset. This will create a series of foreign tables that, when queried,
will forward requests to the Firebase API:

```
$ sgr mount hackernews hackernews
Mounting topstories...
Mounting newstories...
Mounting beststories...
Mounting askstories...
Mounting showstories...
Mounting jobstories...
```

You can now run ordinary SQL queries against these tables:

```
$ sgr sql -s hackernews "SELECT id, title, url, score FROM showstories LIMIT 10"

23573474  Show HN: OBS-web, control OBS from the browser                                    https://github.com/Niek/obs-web                                                                      49
23574723  Launch HN: Mighty Health (YC S19) – Health coaching for people over 50                                                                                                                 17
23575326  Show HN: Jargon-free financial advice via text                                                                                                                                          3
23574926  Show HN: Cedreo easy Floor Plan Software. Instant 3D floor plans from 2D drawing  https://cedreo.com/floor-plan-software/                                                               2
23574842  Show HN: A QEMU port for iOS that you can sideload                                https://getutm.app/                                                                                   2
23569912  Show HN: Noodel.js – User interface for beautiful, dynamic content trees          https://github.com/zlu883/Noodel                                                                     17
23562165  Show HN: Poica – Algebraic data types and type introspection for pure C           https://github.com/Hirrolot/poica                                                                   101
23560046  Show HN: My Book on Evolutionary Algorithms, Written in Python Notebooks          https://shahinrostami.com/posts/search-and-optimisation/practical-evolutionary-algorithms/preface/   59
23572342  Show HN: Portable Serverless without the complexity of Kubernetes                 https://www.openfaas.com/blog/introducing-faasd/                                                      6
23561904  Show HN: Flatend – Quickly build microservices using P2P networking in Node/Go    https://github.com/lithdew/flatend                                                                  134
```

Since actual query planning and filtering is done by PostgreSQL and the foreign data wrapper only
fetches tuples from the API, all PostgreSQL constructs are supported:

```
$ sgr sql -s hackernews "SELECT id, title, url, score FROM topstories \
    WHERE title LIKE '%Python%' ORDER BY score DESC LIMIT 5"

23549273  Practical Python Programming                                              https://github.com/dabeaz-course/practical-python                                                   341
23559680  PyPy: A Faster Python Implementation                                      https://www.pypy.org/index.html                                                                      71
23560046  Show HN: My Book on Evolutionary Algorithms, Written in Python Notebooks  https://shahinrostami.com/posts/search-and-optimisation/practical-evolutionary-algorithms/preface/   59
```
