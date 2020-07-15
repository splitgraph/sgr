#!/bin/bash -e

# Initialize the Splitgraph engine. Splitgraph's sgr mount doesn't need this, but
# cloning/checking out data does.
sgr init

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
sgr mount socrata chicago -o '{"domain": "data.cityofchicago.org"}'

# Set up the foreign tables for the whole Cambridge Open Data endpoint
# https://www.splitgraph.com/cambridgema-gov
sgr mount socrata cambridge -o '{"domain": "data.cambridgema.gov"}'
