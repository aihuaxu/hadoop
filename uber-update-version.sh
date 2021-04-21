#!/bin/bash

BASE_VERSION=2.8.2-slownode-testB
START_COMMIT=042c04d

echo "Calculating the new version number ..."
REVISION=$(git log --oneline | grep $START_COMMIT -n | cut -d ':' -f 1)
NEW_VERSION=${BASE_VERSION}-uber-${REVISION}

echo "Setting new version to be ${NEW_VERSION}"
cd "$(dirname "$0")"
mvn versions:set -DnewVersion=${NEW_VERSION}
sed -i.bak "s/VERSION=.*/VERSION=${NEW_VERSION}/" uber-mvn-deploy.sh
rm uber-mvn-deploy.sh.bak

echo "Cleaning up ..."
find . -name '*.versionsBackup' -exec rm {} \;
echo "Done"
