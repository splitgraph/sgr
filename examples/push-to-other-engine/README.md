# Pushing data between two Splitgraph engines.

## Introduction

Much like Git, Splitgraph allows you to push to and pull datasets from other remote Splitgraph installations.

This example will:

  * Start up two Splitgraph engines and configure them to synchronize with each other.
  * Create a dataset on the local engine
  * Push it out to the remote engine
  * Make a change to the dataset on the local engine
  * Push the changes out again.

## Running the example

Run `../run_example.py example.yaml` and press ENTER when prompted to go through the steps.