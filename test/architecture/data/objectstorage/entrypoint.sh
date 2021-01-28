#!/bin/sh -ex

echo "Copying the CSV test files into the bucket"
mkdir -p /tmp
rm /tmp/test_csv -rf
cp -r /objectstorage/test_csv /tmp/test_csv

echo "Starting Minio"
exec /usr/bin/docker-entrypoint.sh server /tmp
