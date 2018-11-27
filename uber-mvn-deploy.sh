#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

### Copied from start-build-env.sh, with minor changes

set -e               # exit on error

cd "$(dirname "$0")" # connect to root

docker build -t hadoop-build dev-support/docker

if [ "$(uname -s)" == "Linux" ]; then
  USER_NAME=${SUDO_USER:=$USER}
  USER_ID=$(id -u "${USER_NAME}")
  GROUP_ID=$(id -g "${USER_NAME}")
else # boot2docker uid and gid
  USER_NAME=$USER
  USER_ID=1000
  GROUP_ID=50
fi

docker build -t "hadoop-build-${USER_ID}" - <<UserSpecificDocker
FROM hadoop-build
RUN groupadd --non-unique -g ${GROUP_ID} ${USER_NAME}
RUN useradd -g ${GROUP_ID} -u ${USER_ID} -k /root -m ${USER_NAME}
ENV HOME /home/${USER_NAME}
UserSpecificDocker

# By mapping the .m2 directory you can do an mvn install from
# within the container and use the result on your normal
# system.  And this also is a significant speedup in subsequent
# builds because the dependencies are downloaded only once.
docker run --rm=true -i \
  -v "${PWD}:/home/${USER_NAME}/hadoop" \
  -w "/home/${USER_NAME}/hadoop" \
  -v "/mnt/${USER_NAME}:/home/${USER_NAME}" \
  -u "${USER_NAME}" \
  "hadoop-build-${USER_ID}" mvn clean install -DskipTests


### Hack to deploy Uber jars without 'mvn deploy': we don't have
### Jenkins credentials outside the docker box.

VERSION=2.8.2
WORKDIR=$PWD
URL=http://artifactory.uber.internal:4587/artifactory/libs-release-local

function deploy {
  mvn deploy:deploy-file \
      -DgroupId=org.apache.hadoop \
      -DartifactId=$1 \
      -Dversion=${VERSION} \
      -Dclassifier=$2 \
      -DgeneratePom=false \
      -Dpackaging=jar \
      -DrepositoryId=central \
      -Durl=$URL \
      -DpomFile=pom.xml \
      -Dfile=target/$1-${VERSION}.jar
}

function deploy-pom {
  cd $WORKDIR/$1
  mvn deploy:deploy-file \
      -DgroupId=org.apache.hadoop \
      -DartifactId=$2 \
      -Dversion=${VERSION} \
      -DrepositoryId=central \
      -Durl=$URL \
      -DpomFile=pom.xml \
      -Dfile=pom.xml
}

### Deploy each project level POM file individually

deploy-pom . hadoop-main
deploy-pom hadoop-common-project hadoop-common-project
deploy-pom hadoop-hdfs-project hadoop-hdfs-project
deploy-pom hadoop-project hadoop-project
deploy-pom hadoop-mapreduce-project hadoop-mapreduce
deploy-pom hadoop-mapreduce-project/hadoop-mapreduce-client hadoop-mapreduce-client
deploy-pom hadoop-yarn-project hadoop-yarn-project
deploy-pom hadoop-yarn-project/hadoop-yarn hadoop-yarn
deploy-pom hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server hadoop-yarn-server

### Deploy all the jar files individually

cd $WORKDIR
for d in $(find . -name 'target' -type d); do
  cd $WORKDIR
  RESULT=$(find $d -maxdepth 1 -name "hadoop-*-${VERSION}.jar")
  if [[ -n $RESULT ]]; then
    cd $WORKDIR/$d/..
    if [[ -f pom.xml && $(basename $PWD) != 'bkjournal' ]]; then
      deploy $(basename $PWD)

      if [[ $(basename $PWD) == *tests ]]; then
         deploy $(basename $PWD) tests
      fi
    fi
  fi
done
