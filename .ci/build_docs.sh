#!/usr/bin/env bash

CI_DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
REPO_ROOT_DIR="${CI_DIR}/.."

source "$HOME"/.poetry/env

pushd "$REPO_ROOT_DIR" \
    && poetry run "$CI_DIR"/prepare_doc_bundle.sh \
    && popd \
    && exit 0

exit 1
