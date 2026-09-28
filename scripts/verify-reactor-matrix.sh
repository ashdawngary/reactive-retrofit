#!/usr/bin/env sh
set -eu

REACTOR_VERSIONS="3.5.20 3.6.18 3.7.19 3.8.7"

for reactor_version in $REACTOR_VERSIONS; do
  echo "Testing Reactor ${reactor_version}"
  mvn -q clean verify -Dreactor.version="${reactor_version}"
done

echo "Reactor compatibility matrix passed."
