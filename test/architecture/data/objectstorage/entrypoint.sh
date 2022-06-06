#!/bin/sh -ex

echo "Copying the CSV test files into the bucket"
rm /data/test_csv -rf
cp -r /objectstorage/test_csv /data/test_csv

echo "Starting Minio"
exec /usr/bin/docker-entrypoint.sh server /data
