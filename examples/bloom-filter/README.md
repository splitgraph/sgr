# Using a bloom filter to speed up queries

## Introduction

Splitgraph stores tables as chunks backed by [cstore_fdw](https://github.com/citusdata/cstore_fdw) files.
Tables are partitioned by a certain column chosen at image creation time and data on smallest and
largest values for each chunk is kept in the index.

If a query filters on the same dimension as what the table is chunked by
(or queries on columns that rarely overlap in different chunks), it can be made more efficient
by not considering some chunks (including not downloading them at all).

However, in some cases fragments might have unique values but still have their ranges overlap.
The [2016 US Presidential Election dataset](https://www.splitgraph.com/splitgraph/2016_election/) ([source](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/LYWX3D))
is fragmented by precinct ID which has the state FIPS code as its first part. So a query filtering
on the state FIPS code will hit a few fragments, but a query filtering on the name of the county
will need to download and inspect all fragments in the dataset (since almost every fragment will
have county names spanning from A to Z).

Splitgraph allows the user to add an extra [bloom filter](https://en.wikipedia.org/wiki/Bloom_filter)
index on such columns. Bloom filters can use a small amount of space to make a judgement on
whether a set definitely doesn't or might have a certain value. This is perfect for this use case:
with about 1KB of overhead in the bloom filter signature we can determine whether a fragment
contains a certain county name with a 1% false positive rate.

This example will:

* run a fragmentation-friendly query on the state name which will only need to download a small part of the whole
dataset 
* run an SQL EXPLAIN on a query filtering on county name that will need to scan through the whole dataset
* download the dataset and add a bloom filter on the `county_name` column
* run the query again, showing that it will only need to scan through a couple of chunks  

## Running the example

Copy your .sgconfig file into this directory (it must contain API credentials to access
data.splitgraph.com). If you don't have them yet, take a look at the
[Splitgraph Cloud reference](https://www.splitgraph.com/docs/splitgraph_cloud/introduction) or register using `sgr cloud register`.

Then, run `../run_example.py example.yaml` and press ENTER when prompted to go through the steps.