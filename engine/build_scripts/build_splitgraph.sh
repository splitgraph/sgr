#!/bin/bash -ex

# Install the Splitgraph library and the layered querying foreign data wrapper.
cd /splitgraph

export POETRY_VERSION=1.0.5

curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python3.8

# Install globally (otherwise we'll need to find a way to get Multicorn to see the venv)
ln -s /usr/bin/python3.8 /usr/bin/python
source $HOME/.poetry/env
poetry config virtualenvs.create false

poetry export -f requirements.txt --without-hashes -o requirements.txt && sed -i "/^-e/d" requirements.txt
pip install -r requirements.txt
poetry install --no-dev
