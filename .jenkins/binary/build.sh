#!/bin/bash

set -e
set -o pipefail

#######################################################################################################################
# Check all env has been set correctly.                                                                               #
#######################################################################################################################

if [ -z "${ARTIFACTORY_AUTH}" ]; then
  echo 'ERROR: Env ARTIFACTORY_AUTH not set.'
  exit 1
fi

if [ -z "${ARTIFACTS_DIR_NAME}" ]; then
  echo 'ERROR: Env ARTIFACTS_DIR_NAME not set.'
  exit 1
fi

if [ -z "${BRANCH}" ]; then
  echo 'ERROR: Env BRANCH not set.'
  exit 1
fi

if [ -z "${BUILD_NUMBER}" ]; then
  echo 'ERROR: Env BUILD_NUMBER not set.'
  exit 1
fi

if [ -z "${WORKSPACE}" ]; then
  echo 'ERROR: Env WORKSPACE not set.'
  exit 1
fi

#######################################################################################################################
# Check if necessary directory paths exist.                                                                           #
#######################################################################################################################

SRC_CODE="${WORKSPACE}/hadoop"
CI_CODE="${SRC_CODE}/.jenkins"
DEV_DOCKERFILE_DIR="${SRC_CODE}/dev-support/docker"
BUILD_DOCKERFILE_DIR="${CI_CODE}/binary/docker"

if [ ! -d "${SRC_CODE}" ]; then
  echo "ERROR: ${SRC_CODE} does not exist."
  exit 2
fi

if [ ! -d "${DEV_DOCKERFILE_DIR}" ]; then
  echo "ERROR: ${DEV_DOCKERFILE_DIR} does not exist."
  exit 2
fi

if [ ! -d "${BUILD_DOCKERFILE_DIR}" ]; then
  echo "ERROR: ${BUILD_DOCKERFILE_DIR} does not exist."
  exit 2
fi

#######################################################################################################################
# Determine artifact name.                                                                                            #
#######################################################################################################################

echo 'INFO: Determining artifact name.'

pushd ${SRC_CODE} > /dev/null
SRC_CODE_GIT_HASH=$(git rev-parse HEAD)
popd > /dev/null

ARTIFACT_NAME='hadoop.tar.gz'
echo "INFO: Derived artifact name: ${ARTIFACT_NAME}"

#######################################################################################################################
# Clean up artifacts directory.                                                                                       #
#######################################################################################################################

ARTIFACTS_DIR="${WORKSPACE}/${ARTIFACTS_DIR_NAME}"
echo "INFO: Removing and recreating ${ARTIFACTS_DIR}"
rm -rf ${ARTIFACTS_DIR} && mkdir -p ${ARTIFACTS_DIR}

#######################################################################################################################
# Clean up existing images and containers.                                                                            #
#######################################################################################################################

CONTAINER_NAME='hadoop-build-container'
DEV_IMAGE_NAME="hadoop-src:${SRC_CODE_GIT_HASH}"
BUILD_IMAGE_NAME="hadoop-build:${SRC_CODE_GIT_HASH}"

echo "INFO: Stopping and removing existing docker containers of ${CONTAINER_NAME} ..."
existing_containers=$(docker ps -a -q --filter name=${CONTAINER_NAME})
[ -n "${existing_containers}" ] && docker stop ${existing_containers}
[ -n "${existing_containers}" ] && docker rm ${existing_containers}

echo "INFO: Removing existing docker image of ${BUILD_IMAGE_NAME} ..."
existing_image=$(docker images -a -q ${BUILD_IMAGE_NAME})
[ -n "${existing_image}" ] && docker rmi ${existing_image}

echo "INFO: Removing existing docker image of ${DEV_IMAGE_NAME} ..."
existing_image=$(docker images -a -q ${DEV_IMAGE_NAME})
[ -n "${existing_image}" ] && docker rmi ${existing_image}

######################################################################################################################
# Populate BUILD_MANIFEST.txt file before build.                                                                      #
#######################################################################################################################

echo 'INFO: Populating BUILD_MANIFEST...'

BUILD_MANIFEST_FILE="${BUILD_DOCKERFILE_DIR}/BUILD_MANIFEST.txt"
rm -f ${BUILD_MANIFEST_FILE}
cat > ${BUILD_MANIFEST_FILE} <<- EOF
=== Hadoop Binary Build ===
Source Code Git Hash    : ${SRC_CODE_GIT_HASH}
Jenkins Build No        : ${BUILD_NUMBER}
Jenkins Build URL       : ${BUILD_URL}
Jenkins Build Cause     : ${BUILD_CAUSE}
Jenkins Build Date      : $(date)


EOF
chmod 444 ${BUILD_MANIFEST_FILE}

#######################################################################################################################
# Populate DcokerFile file before build.
#######################################################################################################################

echo "INFO: Populating Dockerfile..."

BUILD_DOCKER_FILE="${BUILD_DOCKERFILE_DIR}/Dockerfile"
rm -f ${BUILD_DOCKER_FILE}
cat > ${BUILD_DOCKER_FILE} <<- EOF
# Dockerfile
FROM hadoop-src:${SRC_CODE_GIT_HASH}
ADD binary_build.sh /root/binary_build.sh
ADD BUILD_MANIFEST.txt /root/BUILD_MANIFEST.txt
CMD /root/binary_build.sh


EOF
chmod 444 ${BUILD_DOCKER_FILE}

#######################################################################################################################
# Build the docker images and run container which builds the artifact.                                                #
#######################################################################################################################

echo "INFO: Building docker image ${DEV_IMAGE_NAME} ..."
docker build ${DEV_DOCKERFILE_DIR} -t ${DEV_IMAGE_NAME}

echo "INFO: Building docker image ${BUILD_IMAGE_NAME} ..."
docker build ${BUILD_DOCKERFILE_DIR} -t ${BUILD_IMAGE_NAME}

echo "INFO: Building Hadoop tar with docker container ${CONTAINER_NAME} ..."
HADOOP_VERSION_FILE_NAME='hadoop_version'
docker run --rm -i --name ${CONTAINER_NAME} \
           -e ARTIFACT_NAME=${ARTIFACT_NAME} \
           -e HADOOP_VERSION_FILE_NAME=${HADOOP_VERSION_FILE_NAME} \
           -e BUILD_NUMBER=${BUILD_NUMBER} \
           -e BUILD_URL=${BUILD_URL} \
           -e GIT_COMMIT=${SRC_CODE_GIT_HASH} \
           -v ${SRC_CODE}:/hadoop:ro \
           -v ${ARTIFACTS_DIR}:/artifacts:rw \
           ${BUILD_IMAGE_NAME}

#######################################################################################################################
# Upload artifact to artifactory.                                                                                     #
#######################################################################################################################

echo 'INFO: Artifacts Created:'
ls -al ${ARTIFACTS_DIR}

ARTIFACTORY_URL="http://artifactory.uber.internal:4587/artifactory/pub/hadoop/binary/${SRC_CODE_GIT_HASH}/${ARTIFACT_NAME}"

curl --user "${ARTIFACTORY_AUTH}" \
     ${ARTIFACTORY_URL} \
     --upload-file ${ARTIFACTS_DIR}/${ARTIFACT_NAME}

#######################################################################################################################
# Generate properties file for downstream jobs.                                                                       #
#######################################################################################################################

PROPERTIES_FILE="${WORKSPACE}/build.properties"
rm -f ${PROPERTIES_FILE}
echo "HADOOP_TAR_URL=${ARTIFACTORY_URL}" >> ${PROPERTIES_FILE}
echo "HADOOP_VERSION=$(cat ${ARTIFACTS_DIR}/${HADOOP_VERSION_FILE_NAME})" >> ${PROPERTIES_FILE}

echo 'INFO: Properties file contents:'
cat ${PROPERTIES_FILE}
