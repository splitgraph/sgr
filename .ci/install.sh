#!/bin/bash -ex

poetry export --dev -f requirements.txt --without-hashes -o /tmp/requirements.txt -E pandas
sed -i "/ @ \//d" /tmp/requirements.txt
python -m pip install -U pip
cat /tmp/requirements.txt
pip install --no-deps -r /tmp/requirements.txt
poetry install -E pandas

# Needed to test the dbt example, not required by core sg
python -m venv "$DBT_VENV"
. "$DBT_VENV"/bin/activate
pip install dbt-core==1.0.0 dbt-postgres==1.0.0
pip install --force-reinstall --upgrade markupsafe==2.0.1

# Singer tap integration test
python -m venv "$TAP_MYSQL_VENV"
. "$TAP_MYSQL_VENV"/bin/activate
pip install tap-mysql

# No deactivate here -- Poetry will use a separate venv for Splitgraph.
