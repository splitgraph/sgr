# Pushing data to object storage

## Introduction

When [pushing data to a remote engine](../push-to-other-engine), you can upload Splitgraph objects
to S3-compatible object storage instead of storing them directly on the remote engine. We use
[MinIO](https://min.io/) for this example.

The remote engine has the actual access credentials to the bucket and pre-signs upload/download
URLs for the local engine.

This example will:

  * Start up two Splitgraph engines and configure them to synchronize with each other.
  * Create a dataset on the local engine
  * Push it out to the remote engine, uploading objects to object storage.

## Running the example

Run `../run_example.py example.yaml` and press ENTER when prompted to go through the steps.