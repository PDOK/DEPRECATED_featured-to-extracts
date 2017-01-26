#!/bin/bash

set -ev
export VERSION=$(printf $(cat VERSION))

if [ ! -f artifacts/featured-to-extracts-$VERSION-standalone.jar ]; then
  $lein with-profile +cli build
  cp target/featured-to-extracts-$VERSION-standalone.jar artifacts/
fi


if [ ! -f artifacts/featured-to-extracts-$VERSION-web.jar ]; then
  $lein with-profile +web-jar build
  cp target/featured-to-extracts-$VERSION-web.jar artifacts/
fi
