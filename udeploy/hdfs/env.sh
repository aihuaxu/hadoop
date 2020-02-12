#!/bin/bash

set -e
set -x


if [ -z "${CONTAINER_TYPE}" ]; then
  echo "ERROR: env CONTAINER_TYPE is empty."
  exit 1
fi

if [ -z "${ODIN_TECHNOLOGY}" ]; then
  echo "ERROR: env ODIN_TECHNOLOGY is empty."
  exit 1
fi

if [ -z "${DSC_SERVICE}" ]; then
  echo "ERROR: env DSC_SERVICE is empty."
  exit 1
fi

if [ -z "${DSC_DEPLOYMENT}" ]; then
  echo "ERROR: env DSC_DEPLOYMENT is empty."
  exit 1
fi


export HDFS_PATH='/opt/hdfs'
export HDFS_CONF_PATH="${HDFS_PATH}/conf"
export DSC_TARGET_DIR="${HDFS_PATH}/dsc"
export DSC_SUB_PATH="${ODIN_TECHNOLOGY}/${DSC_SERVICE}/${DSC_DEPLOYMENT}"
export DSC_SETUP_SCRIPT="/shared/dsc/configs/${ODIN_TECHNOLOGY}/setup-config.sh"

export SSL_CLIENT="${HDFS_CONF_PATH}/ssl-client.xml"
export SSL_SERVER="${HDFS_CONF_PATH}/ssl-server.xml"

export LANGLEY_PATH='/langley/current/hdfs'
export TRUSTSTORE_FILE="${LANGLEY_PATH}/hadoop_secure_webhdfs_truststore/truststore"
export TRUSTSTORE_PSD="${LANGLEY_PATH}/hadoop_secure_webhdfs_truststore/password"
export KEYSTORE_FILE="${LANGLEY_PATH}/hadoop_secure_webhdfs_keystore/keystore"
export KEYSTORE_PSD="${LANGLEY_PATH}/hadoop_secure_webhdfs_keystore/password"

export SECURE_STORE_PATH='/opt/hadoop/keytabs'

export HDFS_SCRIPT="${HDFS_PATH}/binary/sbin/hadoop-start.sh"
export HADOOP_ENV="${HDFS_CONF_PATH}/hadoop-env.sh"
