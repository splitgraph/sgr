#!/bin/bash -ex

# Install the Splitgraph library and the layered querying foreign data wrapper.
cd /splitgraph

export POETRY_VERSION=1.0.5

curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python3.7

# Install globally (otherwise we'll need to find a way to get Multicorn to see the venv)
ln -s /usr/bin/python3.7 /usr/bin/python
# shellcheck disable=SC1090
source "$HOME"/.poetry/env
poetry config virtualenvs.create false

# Export the requirements into pip and install them separately (faster than Poetry)
poetry export -f requirements.txt --without-hashes -o requirements.txt && sed -i "/^-e \.\./d" requirements.txt
pip install --no-deps -r requirements.txt

# We don't use pip/poetry here to install the package in "editable" mode as we
# don't care about setuptools entrypoints etc. The Dockerfile just appends
# /splitgraph to the PYTHONPATH.

# Poetry vendors its packages which adds about 70MB to the final image size -- we
# don't need it at this point, so delete it.
rm "$HOME"/.poetry -rf
