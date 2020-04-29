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

export CORE_SITE="${HDFS_CONF_PATH}/core-site.xml"
export HDFS_SITE="${HDFS_CONF_PATH}/hdfs-site.xml"

export LANGLEY_PATH='/langley/current/hdfs'
export TRUSTSTORE_FILE="${LANGLEY_PATH}/hadoop_secure_webhdfs_truststore/truststore"
export TRUSTSTORE_PSD="${LANGLEY_PATH}/hadoop_secure_webhdfs_truststore/password"
export KEYSTORE_FILE="${LANGLEY_PATH}/hadoop_secure_webhdfs_keystore/keystore"
export KEYSTORE_PSD="${LANGLEY_PATH}/hadoop_secure_webhdfs_keystore/password"

export SECURE_STORE_PATH='/opt/hadoop/keytabs'

export HDFS_START_SCRIPT="${HDFS_PATH}/binary/sbin/hadoop-start.sh"
export HDFS_CMD="${HDFS_PATH}/binary/bin/hdfs"
export HADOOP_ENV="${HDFS_CONF_PATH}/hadoop-env.sh"

export ODIN_SECRETS_WEBHDFS_PATH='/secrets/webhdfs'
export ODIN_TRUSTSTORE_FILE="${ODIN_SECRETS_WEBHDFS_PATH}/truststore/truststore"
export ODIN_TRUSTSTORE_PSD="${ODIN_SECRETS_WEBHDFS_PATH}/truststore/password"
export ODIN_KEYSTORE_FILE="${ODIN_SECRETS_WEBHDFS_PATH}/keystore/keystore"
export ODIN_KEYSTORE_PSD="${ODIN_SECRETS_WEBHDFS_PATH}/keystore/password"

export KRB5_CONF_TEMPLATE="krb5.conf.template"
export KRB5_CONF_PATH="/etc/krb5.conf"
