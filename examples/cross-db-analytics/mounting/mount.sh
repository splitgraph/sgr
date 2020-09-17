#!/bin/bash -e

THIS_DIR="$(cd -P -- "$(dirname -- "$0")" && pwd -P)"

echo "Initializing Splitgraph..."
sgr init

echo "Mounting Matomo..."
sgr mount mysql_fdw matomo_raw -c "$MATOMO_USER":"$MATOMO_PASS"@"$MATOMO_HOST":"$MATOMO_PORT" -o@"$THIS_DIR"/matomo.json

echo "Mounting Elasticsearch..."
sgr mount elasticsearch elasticsearch_raw -c "$ELASTICSEARCH_HOST":"$ELASTICSEARCH_PORT" -o@"$THIS_DIR"/elasticsearch.json

echo "Building Matomo model..."
psql $(sgr config -n) -v ON_ERROR_STOP=1 -1 < "$THIS_DIR"/matomo.sql

echo "Building Elasticsearch model..."
psql $(sgr config -n) -v ON_ERROR_STOP=1 -1 < "$THIS_DIR"/elasticsearch.sql
