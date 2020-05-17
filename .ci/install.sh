#!/bin/bash -ex

source "$HOME"/.poetry/env

poetry export --dev -f requirements.txt --without-hashes -o /tmp/requirements.txt -E pandas
sed -i "/^-e/d" /tmp/requirements.txt
pip install --no-deps -r /tmp/requirements.txt
poetry install -E pandas

# Needed to test the dbt example, not required by core sg
pip install dbt

# sudo cat /etc/docker/daemon.json

echo "Building the engine..."
cd engine && make with_postgis=1 && cd ..
