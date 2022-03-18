#!/usr/bin/env bash

DEFAULT_PYPI_URL="https://test.pypi.org/legacy/"

CI_DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
REPO_ROOT_DIR="${CI_DIR}/.."

# By default, will configure PyPi for publishing.
# To skip publishing setup, set NO_PUBLISH=1 .ci/build_wheel.sh
NO_PUBLISH_FLAG="${NO_PUBLISH}"

test -n "$NO_PUBLISH_FLAG" && { echo "Skipping publish because \$NO_PUBLISH is set" ; }
test -z "$PYPI_PASSWORD" && \
    ! test -n "$NO_PUBLISH_FLAG" \
    && { echo "Fatal Error: No PYPI_PASSWORD set. To skip, set NO_PUBLISH=1" ; exit 1 ; }
test -z "$PYPI_URL" && { echo "No PYPI_URL set. Defaulting to ${DEFAULT_PYPI_URL}" ; }

PYPI_URL=${PYPI_URL-"${DEFAULT_PYPI_URL}"}

source "$HOME"/.poetry/env

# Configure pypi for deployment
pushd "$REPO_ROOT_DIR"

set -e
if ! test -n "$NO_PUBLISH_FLAG" ; then
    echo "Configuring poetry with password from \$PYPI_PASSWORD"
    echo "To skip, try: NO_PUBLISH=1 $0 $*"
    poetry config http-basic.testpypi splitgraph "$PYPI_PASSWORD"
    poetry config http-basic.pypi splitgraph "$PYPI_PASSWORD"
fi

# Set the PyPi URL because it can't hurt (we skipped setting the credentials)
poetry config repositories.testpypi "$PYPI_URL"

poetry build
popd

set +e
exit 0
