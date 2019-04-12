#!/usr/bin/env bash

REPO_ROOT_DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
TEST_DIR="${REPO_ROOT_DIR}/test"

exec "$TEST_DIR"/architecture/wait-for-architecture.sh $1
