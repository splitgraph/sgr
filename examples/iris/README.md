# UCIML Iris train/test validation

This example showcases using the Splitgraph API from within Python, as well as using Splitgraph
to seamlessly switch between two copies of an SQL database.

It will:
 
 * ingest the Iris dataset from a CSV file
 * generate a train and test dataset as distinct Splitgraph images
 * use scikit-learn to train a logistic regression classifier on one half of the dataset
 * compare the performance of the model on different datasets

## Running the example

Build and start up the engine:

```
export COMPOSE_PROJECT_NAME=splitgraph_example 
docker-compose down -v
docker-compose build
docker-compose up -d
sgr init
```

Install this package with [Poetry](https://github.com/sdispater/poetry): `poetry install`

Open the notebook in Jupyter: `jupyter notebook iris.ipynb`
