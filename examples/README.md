# Splitgraph example projects

## Introduction

This subdirectory contains various self-contained projects and snippets that can be used
as templates for your own experiments with Splitgraph.

Each example has a README file. Some of these examples get or push data from/to the Splitgraph registry at data.splitgraph.com and so require you to be logged into it.

## Contents

  * [get-started](./get-started): Register on Splitgraph Cloud and start up a Splitgraph engine.
  * [import-from-csv](./import-from-csv): Import data from a CSV file into a Splitgraph image.
  * [import-from-mongo](./import-from-mongo): Import data from MongoDB into Splitgraph.
  * [push-to-other-engine](./push-to-other-engine): Share data with other Splitgraph engines.
  * [iris](./iris): Manipulate and query Splitgraph data from a Jupyter notebook.
  * [bloom-filter](./bloom-filter): Showcase using bloom filters to query large datasets with a limited amount of cache.
  * [splitfiles](./splitfiles): Use Splitfiles to build Splitgraph data images, track their provenance and keep them up to date.
  * [query_api](./query_api): Try out the REST API that gets generated for every dataset on Splitgraph Cloud.
  * [us-election](./us-election): A real-world Splitfile example that joins multiple datasets.
  * [sample_splitfiles](./sample_splitfiles): A collection of loose Splitfiles that run against interesting datasets on Splitgraph Cloud.

## Contributing

The template example in [templates](./template) has a sample `example.yaml` file. Alternatively, you can copy one of the existing examples. Note that most examples that use the `example.yaml` format and don't require logging into Splitgraph are tested with a suite in the [test](./test) subdirectory.

In addition, all examples have [Asciinema casts](https://asciinema.org/) generated for them automatically at release time which are then available to be embedded into the website, see [the script](../.ci/rebuild_asciicasts.sh).
