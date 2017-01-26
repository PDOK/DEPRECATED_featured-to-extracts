#!/bin/bash

if [ -z "$TRAVIS_TAG" ]; then

    set -ev
    export VERSION=$(printf $(cat VERSION))

    docker login -u="$DOCKER_USERNAME" -p="$DOCKER_PASSWORD"

    if [ ! -f artifacts/featured-to-extracts-$VERSION-standalone.jar ]; then
      $lein with-profile +cli build
      cp target/featured-to-extracts-$VERSION-standalone.jar artifacts/
    fi
    cp artifacts/featured-to-extracts-$VERSION-standalone.jar artifacts/featured-to-extracts-cli.jar

    docker build --build-arg version=$VERSION . -f docker-cli/Dockerfile -t pdok/featured-to-extracts-cli:$TRAVIS_COMMIT
    docker push pdok/featured-to-extracts-cli:$TRAVIS_COMMIT

    if [ ! -f artifacts/featured-to-extracts-$VERSION-web.jar ]; then
      $lein with-profile +web-jar build
      cp target/featured-to-extracts-$VERSION-web.jar artifacts/
    fi
    cp artifacts/featured-to-extracts-$VERSION-web.jar artifacts/featured-to-extracts-web.jar

    docker build --build-arg version=$VERSION . -f docker-web/Dockerfile -t pdok/featured-to-extracts-web:$TRAVIS_COMMIT
    docker push pdok/featured-to-extracts-web:$TRAVIS_COMMIT

fi
