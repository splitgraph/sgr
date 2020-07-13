# Splitgraph adapter for dbt

This is a package that adds support for referencing Splitgraph images in your dbt models when
running dbt on the Splitgraph engine.

## Installation

Run `pip install dbt-splitgraph`.

If you wish to develop this package, you can run `python setup.py develop` from this directory.

## Usage

Add credentials for your Splitgraph engine to the dbt `profiles.yml` file (see the [dbt documentation](https://docs.getdbt.com/docs/running-a-dbt-project/using-the-command-line-interface/configure-your-profile/) on information on the profile file):

```yaml
default:
  target: splitgraph
  outputs:
    splitgraph:
      type: splitgraph
      host: localhost
      user: sgr
      pass: password
      port: 5432
      dbname: splitgraph
      schema: [some_schema]
      threads: 4
```

Make sure that the engine is initialized and that your `.sgconfig` file is either in
`~/.splitgraph/.sgconfig` or pointed to by the `SG_CONFIG_FILE` environment variable. If required,
also log into the Splitgraph registry or your custom remote with Splitgraph images using
`sgr cloud login` or `sgr cloud login-api`.

You can now reference Splitgraph images in your dbt models by schema-qualifying tables
with the full Splitgraph image path (you can use hashes or tags). For example:

```sql
{{ config(materialized='table') }}

with source_data as (

    select domain, count(domain) as count
    from "splitgraph/socrata:latest".datasets
    group by domain

)

select *
from source_data

```

In addition, you can run joins on multiple Splitgraph images or between Splitgraph images and other dbt
models.

The semantics for resolving the image are the same as for using it in Splitfiles: it needs to 
exist in one of the remotes specified by `SG_REPO_LOOKUP` or on your local engine. The image doesn't
need to have been cloned beforehand. Queries are satisfied with [layered querying](https://www.splitgraph.com/docs/large-datasets/layered-querying), downloading only the necessary table regions in the background.

## Sample project

The [`./sample_project`](./sample_project) directory contains an example dbt project that runs a
simple model on Splitgraph data. Configure your `.dbt` directory with the correct Splitgraph
credentials as described in the previous section, then run the model:

```bash
$ cd sample_project
$ dbt run    # add `--profiles-dir ./.dbt` 
             # to use the sample profile

Running with dbt=0.17.0                                                                     
Found 1 model, 0 tests, 0 snapshots, 0 analyses, 151 macros, 0 operations, 0 seed files, 0 sources                         
                                                                                                                                                                                                                                                                          
14:35:23 | Concurrency: 4 threads (target='splitgraph')                                                   
14:35:23 |                                                                                  
14:35:23 | 1 of 1 START table model adapter_showcase.use_splitgraph_data........ [RUN]                 
14:35:24 | 1 of 1 OK created table model adapter_showcase.use_splitgraph_data... [SELECT 206 in 1.42s]                     
14:35:24 |                                                                                             
14:35:24 | Finished running 1 table model in 1.54s.   
```

## Internals

This adapter is mostly based on the official dbt PostgreSQL and Redshift adapters. All macros
macros currently just call out to PostgreSQL ones. The actual query execution is performed by
rewriting the SQL statement (using regular expressions) to be in terms of temporary schemas
that Splitgraph images are mounted into as lightweight shims (foreign tables).
