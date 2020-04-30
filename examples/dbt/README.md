# Building Splitgraph datasets with dbt

The recommended way of building Splitgraph data images is [Splitfiles](../splitfiles) that offer
Dockerfile-like caching, provenance tracking and efficient rebuilds.

However, there are plenty of other great tools for transforming data inside data warehouses and,
as long as they work with PostgreSQL, they too can benefit from Splitgraph's data versioning, packaging and sharing capabilities.

One such tool is [dbt](https://getdbt.com) that essentially works as a really advanced SQL templating
engine, allowing to create data transformations from small building blocks and decreasing the amount
of boilerplate.

Turning the source and the target schemas that dbt uses into Splitgraph repositories opens up a lot
of opportunities:

  * No need to run development and production dbt models against the same warehouse. A Splitgraph image can be cloned and checked out on the development engine.
  * Performing what-if analyses becomes simple by switching between different versions of the
    source dataset and comparing the resultant images with `sgr diff`.
  * Built datasets can be pushed to other Splitgraph engines, shared publicly or serve as inputs
    to a pipeline of Splitfiles.  
  * Input datasets can leverage Splitgraph's layered querying, allowing dbt to seamlessly query
    huge datasets with a limited amount of local disk space.  

This example will:

* Ingest the raw orders and customers data from the dbt [Jaffle Shop example](https://github.com/fishtown-analytics/jaffle_shop/tree/master)
* Create an updated version of the original dataset (changing some orders' state to "returned").
* Run dbt against the original dataset to build the dbt model, committing the result as a Splitgraph image.
* Run dbt against the new dataset, committing the result as another Splitgraph image.
* Compare the two versions of the built model against each other.

## Running the example

`../run_example.py example.yaml` and press ENTER when prompted to go through the steps.
