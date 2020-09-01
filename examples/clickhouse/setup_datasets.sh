#!/bin/bash -e

# Alias sgr to the CLI installed inside of the engine container so that the user
# doesn't have to install it locally.
sgr() {
  docker-compose exec -T splitgraph sgr $@
}

# Initialize the Splitgraph engine. Splitgraph's sgr mount doesn't need this, but
# cloning/checking out data does.
sgr init

echo "Setting up Socrata dataset mounts..."

# Set up the Socrata Chicago fire stations dataset by mounting it with sgr mount
# Reference: https://www.splitgraph.com/docs/ingesting-data/socrata#usage
sgr mount socrata chicago_data -o @- <<EOF
{
    "domain": "data.cityofchicago.org",
    "tables": {"fire_stations": "28km-gtjn"}
}
EOF

# Set up the foreign tables for the whole Chicago Open Data endpoint
# https://www.splitgraph.com/cityofchicago
sgr mount socrata chicago -o @- <<EOF
{"domain": "data.cityofchicago.org"}
EOF

# Set up the foreign tables for the whole Cambridge Open Data endpoint
# https://www.splitgraph.com/cambridgema-gov
sgr mount socrata cambridge -o  @- <<EOF
{"domain": "data.cambridgema.gov"}
EOF

echo "Logging into the Splitgraph registry..."
sgr cloud login-api

echo "Cloning the https://www.splitgraph.com/splitgraph/2016_election/ dataset"
sgr clone splitgraph/2016_election
sgr checkout --layered splitgraph/2016_election:latest
