#!/bin/bash

set -ev
export VERSION=$(printf $(cat VERSION))

docker login -u="$DOCKER_USERNAME" -p="$DOCKER_PASSWORD"

cp artifacts/featured-to-extracts-$VERSION-standalone.jar artifacts/featured-to-extracts-cli.jar
docker build --build-arg version=$VERSION . -f docker-cli/Dockerfile -t pdok/featured-to-extracts-cli:$VERSION
docker push pdok/featured-to-extracts-cli:$VERSION

cp artifacts/featured-to-extracts-$VERSION-web.jar artifacts/featured-to-extracts-web.jar
docker build --build-arg version=$VERSION . -f docker-web/Dockerfile -t pdok/featured-to-extracts-web:$VERSION
docker push pdok/featured-to-extracts-web:$VERSION
