#!/usr/bin/env bash

CI_DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
REPO_ROOT_DIR="${CI_DIR}/.."

test -z "$COMPOSE_VERSION" && { echo "Fatal Error: No COMPOSE_VERSION set" ; exit 1 ; }

D_COMPOSE_BASE_URL="https://github.com/docker/compose/releases/download"
D_COMPOSE_ARCH="docker-compose-$(uname -s)-$(uname -m)"
D_COMPOSE_URL="${D_COMPOSE_BASE_URL}/${COMPOSE_VERSION}/${D_COMPOSE_ARCH}"
export POETRY_VERSION=1.0.0

# Install docker compose and poetry
pushd "$REPO_ROOT_DIR" \
    && curl -L "$D_COMPOSE_URL" > docker-compose \
    && chmod +x docker-compose \
    && sudo mv docker-compose /usr/local/bin \
    && pip install poetry=="$POETRY_VERSION" \
    && poetry config virtualenvs.create false \
    && popd \
    && exit 0

# Load the engine image from cache, if it exists.
docker load -i docker_images/engine.tar || true

exit 1
