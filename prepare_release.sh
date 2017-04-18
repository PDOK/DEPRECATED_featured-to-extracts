#!/usr/bin/env bash

set -e

echo "Current version  $(cat VERSION)"

printf "Release version? "
read release_version

printf $release_version > VERSION
git add VERSION
git commit -q -m "Prepare release $release_version"
git tag -a featured-to-extracts-$release_version -m "Release $release_version"

printf "New development version? "
read dev_version

printf $dev_version > VERSION
git add VERSION
git commit -q -m "New release cycle $dev_version [ci skip]"

git push --follow-tags
