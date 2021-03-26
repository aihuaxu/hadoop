#!/bin/bash -ex

BASE_VERSION=2.8.2

if [[ -z "$RELEASE_VERSION" ]]; then
    NEW_VERSION="uber-hadoop-$BASE_VERSION-$(git rev-parse --short=16 HEAD)"
else
    NEW_VERSION="uber-hadoop-$BASE_VERSION-$RELEASE_VERSION"
fi

echo "Setting new version to be ${NEW_VERSION}"
cd "$(dirname "$0")"
mvn versions:set -DnewVersion=${NEW_VERSION}
sed -i.bak "s/VERSION=.*/VERSION=${NEW_VERSION}/" uber-mvn-deploy.sh
echo "Cleaning up ..."
mvn versions:commit
echo "Done"
