#!/usr/bin/env bash

DEFAULT_PYPI_URL="https://test.pypi.org/legacy/"

CI_DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
REPO_ROOT_DIR="${CI_DIR}/.."

test -z "$PYPI_PASSWORD" && { echo "Fatal Error: No PYPI_PASSWORD set" ; exit 1 ; }
test -z "$PYPI_URL" && { echo "No PYPI_URL set. Defaulting to ${DEFAULT_PYPI_URL}" ; }

PYPI_URL=${PYPI_URL-"${DEFAULT_PYPI_URL}"}

source "$HOME"/.poetry/env

# Configure pypi for deployment
pushd "$REPO_ROOT_DIR" \
    && poetry config repositories.testpypi "$PYPI_URL" \
    && poetry config http-basic.testpypi splitgraph "$PYPI_PASSWORD" \
    && poetry config http-basic.pypi splitgraph "$PYPI_PASSWORD" \
    && poetry build \
    && poetry run "$CI_DIR"/prepare_doc_bundle.sh \
    && popd \
    && exit 0

exit 1
