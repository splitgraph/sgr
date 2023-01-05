#!/bin/bash -ex

# Install the Splitgraph library and the layered querying foreign data wrapper.
cd /splitgraph

export POETRY_VERSION=1.1.6

curl -sSL https://install.python-poetry.org | python3.7 -
export PATH="/root/.local/bin:$PATH"

# Install globally (otherwise we'll need to find a way to get Multicorn to see the venv)
ln -sf /usr/bin/python3.7 /usr/bin/python
poetry config virtualenvs.create false

# Export the requirements into pip and install them separately (faster than Poetry)
poetry export -f requirements.txt --without-hashes -o requirements.txt && sed -i "/ @ \//d" requirements.txt
pip install --no-deps -r requirements.txt

# We don't use pip/poetry here to install the package in "editable" mode as we
# don't care about setuptools entrypoints etc. The Dockerfile just appends
# /splitgraph to the PYTHONPATH.

# Poetry vendors its packages which adds about 70MB to the final image size -- we
# don't need it at this point, so delete it.
rm "$HOME"/.poetry -rf
