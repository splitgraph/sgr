#!/bin/bash -ex

# Push the built image out to Docker Hub

docker login -u "$DOCKER_USERNAME" -p "$DOCKER_PASSWORD"

# Determine the tags we're pushing the image out under.

# If the Git commit tag is specified, treat this as a release and push
# the image out with that tag + "stable" + "latest".

# If it's not specified (we're assuming this is only run from master builds),
# push the image out under "latest".

# We could also do something like what Python does and push out the major, minor and
# release tags, e.g. a build of 3.4.5 would push out splitgraph/engine:3.4.5,
# splitgraph/engine:3.4 and splitgraph/engine:latest.

if [ -n "$TRAVIS_TAG" ]; then
  # Strip the first letter (v0.0.1 etc) from the Git tag to get the Docker tag (actual version)
  TAGS="latest ${TRAVIS_TAG:1} stable"
else
  TAGS="latest"
fi

source="$DOCKER_REPO"/"$DOCKER_ENGINE_IMAGE":"$DOCKER_TAG"
for tag in $TAGS; do
  target="$DOCKER_REPO"/"$DOCKER_ENGINE_IMAGE":$tag
  docker tag "$source" "$target"
  docker push "$target"
done
