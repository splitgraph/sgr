# Using Splitfiles to build Splitgraph images

This example will:

* Create a source Splitgraph image in the same way as in the CSV ingestion example
* Use a Splitfile to create a monthly summary of weather at RDU airport
* Inspect the image's provenance
* Alter the data to pretend a "revision" has been issued
* Rebuild the image against the new data from its provenance

## Running the example

`../run_example.py example.yaml` and press ENTER when prompted to go through the steps.
