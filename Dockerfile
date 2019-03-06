FROM python:3.6-slim

RUN apt-get update && apt-get install -y curl

RUN curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python

RUN mkdir /splitgraph
COPY . /splitgraph

RUN $HOME/.poetry/bin/poetry config settings.virtualenvs.create false
RUN cd /splitgraph && $HOME/.poetry/bin/poetry install --no-dev

CMD sgr