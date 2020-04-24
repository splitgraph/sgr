# Using Splitgraph as a PostgreSQL replication subscriber

It is possible to add a Splitgraph engine as a replication client to a production PostgreSQL
database, occasionally committing the changes as new Splitgraph images. This is done through
[PostgreSQL logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
and has many uses:

  * Recording the history of the upstream database for audit purposes
  * Using anonymized production data snapshots for integration testing
  * Building derivative datasets with Splitfiles as an alternative to data warehousing.
  
This example will:

* Spin up a PostgreSQL database with some sample data
* Set up a Splitgraph engine as a replication client
* Create an image from the data received from the origin database
* Make changes to the origin database, which will get propagated to the engine
* Record the changes as a new image

## Running the example

`../run_example.py example.yaml` and press ENTER when prompted to go through the steps.
