#!/bin/bash

HDFS_PATH='/opt/hdfs'
HDFS_CONF_PATH="${HDFS_PATH}/conf"
DSC_TARGET_DIR="${HDFS_PATH}/dsc"
DSC_SUB_PATH="${ODIN_TECHNOLOGY}/${DSC_SERVICE}/${DSC_DEPLOYMENT}"
DSC_SETUP_SCRIPT="/shared/dsc/configs/${ODIN_TECHNOLOGY}/setup-config.sh"

SSL_CLIENT="${CONF_BASE}/ssl-client.xml"
SSL_SERVER="${CONF_BASE}/ssl-server.xml"

LANGLEY_PATH='/langley/current/hdfs'
TRUSTSTORE_FILE="${LANGLEY_PATH}/hadoop_secure_webhdfs_truststore/truststore"
TRUSTSTORE_PSD="${LANGLEY_PATH}/hadoop_secure_webhdfs_truststore/password"
KEYSTORE_FILE="${LANGLEY_PATH}/hadoop_secure_webhdfs_keystore/keystore"
KEYSTORE_PSD="${LANGLEY_PATH}/hadoop_secure_webhdfs_keystore/password"

SECURE_STORE_PATH='/opt/hadoop/keytabs'

HDFS_SCRIPT="${HDFS_PATH}/binary/sbin/hadoop-start.sh"
HADOOP_ENV="${HDFS_CONF_PATH}/hadoop-env.sh"

mkdir -p /opt/hdfs/conf
# Update ssl-server.xml and ssl-client.xml with passwords
if [ -f ${TRUSTSTORE_PSD} ] ; then
  truststore_passwd=$(sudo cat ${TRUSTSTORE_PSD})
  [ -f ${SSL_CLIENT} ] && sed -i "s|PLACE_HOLDER_FOR_TRUSTSTORE_PASSWORD|${truststore_passwd}|" ${SSL_CLIENT}
  [ -f ${SSL_SERVER} ] && sed -i "s|PLACE_HOLDER_FOR_TRUSTSTORE_PASSWORD|${truststore_passwd}|" ${SSL_SERVER}
fi
if [ -f ${KEYSTORE_PSD} ] ; then
  keystore_passwd=$(sudo cat ${KEYSTORE_PSD})
  [ -f ${SSL_SERVER} ] && sed -i "s|PLACE_HOLDER_FOR_KEYSTORE_PASSWORD|${keystore_passwd}|" ${SSL_SERVER}
fi

if [ ! -d ${SECURE_STORE_PATH} ]; then
  mkdir -p "${SECURE_STORE_PATH}"
fi

if [ -f ${TRUSTSTORE_FILE} ] ; then
    sudo cp ${TRUSTSTORE_FILE} ${SECURE_STORE_PATH}/truststore.jks
    sudo chmod 444 ${SECURE_STORE_PATH}/truststore.jks
fi
if [ -f ${KEYSTORE_FILE} ] ; then
    sudo cp ${KEYSTORE_FILE} ${SECURE_STORE_PATH}/server.keystore
    sudo chmod 440 ${SECURE_STORE_PATH}/server.keystore
fi

sudo chown -R udocker:udocker ${SECURE_STORE_PATH}

bash ${DSC_SETUP_SCRIPT} ${DSC_TARGET_DIR}
rsync --exclude *.sha -av ${DSC_TARGET_DIR}/${DSC_SUB_PATH}/* ${HDFS_CONF_PATH}/
cp ./udeploy/hdfs/topology.py ${HDFS_CONF_PATH}/