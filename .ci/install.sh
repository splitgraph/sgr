#!/bin/bash -ex

source "$HOME"/.poetry/env

poetry export --dev -f requirements.txt --without-hashes -o /tmp/requirements.txt -E pandas
sed -i "/ @ \//d" /tmp/requirements.txt
pip install --no-deps -r /tmp/requirements.txt
poetry install -E pandas

# Needed to test the dbt example, not required by core sg
python -m venv "$DBT_VENV"
. "$DBT_VENV"/bin/activate
pip install dbt

# Singer tap integration test
python -m venv "$TAP_MYSQL_VENV"
. "$TAP_MYSQL_VENV"/bin/activate
pip install tap-mysql

# No deactivate here -- Poetry will use a separate venv for Splitgraph.

# sudo cat /etc/docker/daemon.json

echo "Building the engine..."
cd engine && make with_postgis=1 && cd ..
