#!/bin/bash

export PATH="~/.pyenv/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

pyenv install 3.6.8
pyenv virtualenv 3.6.8 splitgraph
pyenv activate splitgraph

# Install the Splitgraph library and the layered querying foreign data wrapper.
# We could use the Postgres entrypoint system instead but it's run as the non-root user
# and this container isn't for the sgr tool anyway.

cd /splitgraph

curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python
# Install globally (otherwise we'll need to find a way to get Multicorn to see the venv)
source $HOME/.poetry/env
poetry config settings.virtualenvs.create false
poetry install --no-dev
