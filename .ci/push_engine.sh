#!/bin/bash -ex

# Push the built image out to Docker Hub

docker login -u "$DOCKER_USERNAME" -p "$DOCKER_PASSWORD"

# The engine that's pointed to by $DOCKER_ENGINE_TAG was built with postgis. We want to build
# a version without postgis as well and swap the tags we're pushing it out under. So the engine
# $DOCKER_ENGINE_TAG will be the one without PostGIS and the engine $DOCKER_ENGINE_TAG-postgis
# is the one with it.

docker tag "$DOCKER_REPO"/"$DOCKER_ENGINE_IMAGE":"$DOCKER_TAG" \
  "$DOCKER_REPO"/"$DOCKER_ENGINE_IMAGE":"$DOCKER_TAG"-postgis

cd engine
DOCKER_CACHE_TAG=$DOCKER_TAG-postgis make build
cd ..

# Determine the tags we're pushing the image out under.

# If the Git commit tag is specified, treat this as a release and push
# the image out with that tag + "stable" + "latest".

# If it's not specified (we're assuming this is only run from master builds),
# push the image out under "latest".

# We could also do something like what Python does and push out the major, minor and
# release tags, e.g. a build of 3.4.5 would push out splitgraph/engine:3.4.5,
# splitgraph/engine:3.4 and splitgraph/engine:latest.

if [ -n "$GITHUB_REF" ]; then
  # Strip refs/tags/v .. -- 11 chars from the Git ref to get the Docker tag (actual version)
  TAGS="latest ${GITHUB_REF:11} stable"
else
  TAGS="latest"
fi

source="$DOCKER_REPO"/"$DOCKER_ENGINE_IMAGE":"$DOCKER_TAG"
for tag in $TAGS; do
  target="$DOCKER_REPO"/"$DOCKER_ENGINE_IMAGE":$tag
  docker tag "$source" "$target"
  docker tag "$source"-postgis "$target"-postgis
  docker push "$target"
  docker push "$target"-postgis
done
