FROM python:3.6-slim

RUN apt-get update && apt-get install -y curl

RUN curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python

RUN mkdir /splitgraph
COPY . /splitgraph

RUN $HOME/.poetry/bin/poetry config settings.virtualenvs.create true
RUN cd /splitgraph && $HOME/.poetry/bin/poetry install --no-dev

# The pip-wheel-metadata is supposed to be temporary. For downstream image builds, Poetry tries to reinstall Splitgraph
# from /splitgraph again and fails with
#
# FileExistsError: [Errno 17] File exists: '/splitgraph/pip-wheel-metadata
# /splitgraph-0.0.0.dist-info'
# See https://github.com/pypa/pip/issues/6213
RUN rm /splitgraph/pip-wheel-metadata -rf

CMD sgr
