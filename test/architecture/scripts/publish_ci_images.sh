#!/bin/bash

usage() {
    echo
    echo "./scripts/publish_ci_images.sh"
    echo
    echo "Push the images in docker-compose.ci.yml to docker hub"
    echo
    echo "Script will print tag names and then prompt to login before pushing,"
    echo "so you have a chance to cancel it with ctrl+c if necessary."
    echo
}

test "$1" == "--help" && { usage ; exit 1 ; }

set -eo pipefail

trap 'echo "Canceled by user"; exit 1;' INT

_log_error() {
    echo "$@" 1>&2
}

fatal_error() {
    _log_error "Fatal:" "$@"
    exit 1
}

_log() {
    echo "$@"
}

DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

ARCH_DIR="$DIR"/../

cd "$ARCH_DIR" || {
    fatal_error "Failed to cd to architecture directory at $ARCH_DIR" ;
}

which docker-compose >/dev/null 2>&1|| {
    fatal_error "docker-compose is not installed or not in \$PATH";
}

# Get the CI image tags built from this docker-compose file, e.g.
# splitgraphci/pgorigin
get_ci_image_tags() {
    docker-compose -f docker-compose.ci.yml images -q \
        | xargs docker inspect -f='{{.RepoTags}}' \
        | grep -Eo 'splitgraphci/(.*?):' \
        | cut -d ':' -f1
}

_log "Publishing CI tags:"
get_ci_image_tags

_log "Log into docker hub when prompted"
docker login

while IFS=' ' read -r image_tag ; do
    echo "Publish Image: $image_tag"
    docker push "$image_tag" \
        || fatal_error "Failed to publish ${image_tag}. Stopping prematurely."
done < <(get_ci_image_tags)

echo "Success."
