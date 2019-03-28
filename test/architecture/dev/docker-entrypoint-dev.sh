#!/usr/bin/env bash

install_latest_sgr() {
    mkdir -p /src/venvs \
        && poetry config settings.virtualenvs.create true \
        && poetry config settings.virtualenvs.path /src/venvs \
        && cd /src/splitgraph \
        && poetry install --develop sgr
}

poetry() {
    "$HOME"/.poetry/bin/poetry "$@"
}

install_latest_sgr "$@"

if test "$1" = "shell" ; then
    exec "/bin/bash"
else
    exec "$HOME"/.poetry/bin/poetry run sgr "$@"
fi
