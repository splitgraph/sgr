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
$ sgr sql -s hackernews "SELECT id, title, url, score FROM topstories LIMIT 10"

23648942  Amazon to pay $1B+ for Zoox                                             https://www.axios.com/report-amazon-to-pay-1-billion-for-self-driving-tech-firm-zoox-719d293b-3799-4315-a573-a226a58bb004.html                              55
23646158  When you type realty.com into Safari it takes you to realtor.com        https://www.facebook.com/story.php?story_fbid=10157161487396994&id=501751993                                                                               653
23648864  Turn recipe websites into plain text                                    https://plainoldrecipe.com/                                                                                                                                 30
23644253  Olympus quits camera business after 84 years                            https://www.bbc.com/news/technology-53165293                                                                                                               548
23648217  Boston bans use of facial recognition technology                        https://www.wbur.org/news/2020/06/23/boston-facial-recognition-ban                                                                                          51
23646953  Curl Wttr.in                                                            https://github.com/chubin/wttr.in                                                                                                                          190
23646164  Quora goes permanently remote-first                                     https://twitter.com/adamdangelo/status/1276210618786168833                                                                                                 267
23646395  Dwarf Fortress Creator Explains Its Complexity and Origins [video]      https://www.youtube.com/watch?v=VAhHkJQ3KgY                                                                                                                152
23645305  Blackballed by PayPal, Sci-Hub switches to Bitcoin                      https://www.coindesk.com/blackballed-by-paypal-scientific-paper-pirate-takes-bitcoin-donations                                                             479
23646028  The Acorn Archimedes was the first desktop to use the ARM architecture  https://spectrum.ieee.org/tech-talk/consumer-electronics/gadgets/why-wait-for-apple-try-out-the-original-arm-desktop-experience-today-with-a-raspberry-pi  111

```

Since actual query planning and filtering is done by PostgreSQL and the foreign data wrapper only
fetches tuples from the API, all PostgreSQL constructs are supported:

```
$ sgr sql -s hackernews "SELECT id, title, url, score FROM showstories ORDER BY score DESC LIMIT 5"
23643096  Show HN: Aviary.sh – A tiny Bash alternative to Ansible                           https://github.com/team-video/aviary.sh  235
23626167  Show HN: HN Deck – An alternative way to browse Hacker News                       https://hndeck.sagunshrestha.com/        110
23640069  Show HN: Sourceful – Crowdsourcing the best public Google docs                    https://sourceful.co.uk                  102
23627066  Show HN: Splitgraph - Build and share data with Postgres, inspired by Docker/Git  http://www.splitgraph.com                 79
23629125  Show HN: Deta – A cloud platform for building and deploying apps                  https://www.deta.sh/                      78
```
