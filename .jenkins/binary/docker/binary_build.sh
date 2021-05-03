#!/bin/bash

set -e
set -o pipefail

#######################################################################################################################
# Initial checks.                                                                                                     #
#######################################################################################################################

ARTIFACTS_DIR=/artifacts

if [ ! -d "${ARTIFACTS_DIR}" ]; then
  echo "ERROR: Artifacts directory ${ARTIFACTS_DIR} does not exist."
  exit 1
fi

if [ -z "${ARTIFACT_NAME}" ]; then
  echo 'ERROR: Env ARTIFACT_NAME not set.'
  exit 1
fi

if [ -z "${HADOOP_VERSION_FILE_NAME}" ]; then
  echo 'ERROR: Env HADOOP_VERSION_FILE not set.'
  exit 1
fi

#######################################################################################################################
# Build the Hadoop Binary.                                                                                            #
#######################################################################################################################

mkdir -p /build_dir
rsync -a /hadoop /build_dir

pushd /build_dir/hadoop
mvn -X -B clean package \
    -Pdist,native \
    -Drequire.snappy \
    -Dbundle.snappy \
    -Dsnappy.lib=/usr/lib \
    -Dtar \
    -DskipTests \
    -Dmaven.javadoc.skip=true

HADOOP_VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate \
                     -Dexpression=project.version \
                     -q -DforceStdout)
popd

#######################################################################################################################
# Rename the tar artifact.                                                                                            #
#######################################################################################################################

TEMP_ARTIFACT_DIR='/temp_tar'

mkdir -p ${TEMP_ARTIFACT_DIR}

cp /build_dir/hadoop/hadoop-dist/target/*.tar.gz ${TEMP_ARTIFACT_DIR}
rm -rf /build_dir

pushd ${TEMP_ARTIFACT_DIR}

tar_file_name=$(ls *tar.gz | head -1)
tar -xzf ${tar_file_name}
tar_file_dir=${tar_file_name%.tar.gz}

required_tar_file_name=${ARTIFACT_NAME%.tar.gz}
mv ${tar_file_dir} ${required_tar_file_name}
mv /root/BUILD_MANIFEST.txt ${required_tar_file_name}
tar -czf ${ARTIFACTS_DIR}/${ARTIFACT_NAME} ${required_tar_file_name}

popd

chmod 777 ${ARTIFACTS_DIR}/${ARTIFACT_NAME}
echo "${HADOOP_VERSION}" > ${ARTIFACTS_DIR}/${HADOOP_VERSION_FILE_NAME}