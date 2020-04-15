#!/bin/bash

set -e
set -o pipefail

mkdir -p ${HDFS_CONF_PATH}

# Copy over dsc configs to target path.
bash ${DSC_SETUP_SCRIPT} ${DSC_TARGET_DIR}
rsync --exclude *.sha -av ${DSC_TARGET_DIR}/${DSC_SUB_PATH}/* ${HDFS_CONF_PATH}/
cp ./udeploy/hdfs/topology.py ${HDFS_CONF_PATH}/
cp -r ./udeploy/hdfs/jsvc ${HDFS_CONF_PATH}/jsvc

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

# fetch keytab and langley secrets from files landed by odin-hdfs-worker
if [ -f ${ODIN_TRUSTSTORE_PSD} ] && [ -s ${ODIN_TRUSTSTORE_PSD} ]; then
  truststore_passwd=$(sudo cat ${ODIN_TRUSTSTORE_PSD})
  [ -f ${SSL_CLIENT} ] && sed -i "s|PLACE_HOLDER_FOR_TRUSTSTORE_PASSWORD|${truststore_passwd}|" ${SSL_CLIENT}
  [ -f ${SSL_SERVER} ] && sed -i "s|PLACE_HOLDER_FOR_TRUSTSTORE_PASSWORD|${truststore_passwd}|" ${SSL_SERVER}
fi
if [ -f ${ODIN_KEYSTORE_PSD} ] && [ -s ${ODIN_KEYSTORE_PSD} ]; then
  keystore_passwd=$(sudo cat ${ODIN_KEYSTORE_PSD})
  [ -f ${SSL_SERVER} ] && sed -i "s|PLACE_HOLDER_FOR_KEYSTORE_PASSWORD|${keystore_passwd}|" ${SSL_SERVER}
fi

if [ -f ${ODIN_TRUSTSTORE_FILE} ] && [ -s ${ODIN_TRUSTSTORE_FILE} ]; then
    sudo cp ${ODIN_TRUSTSTORE_FILE} ${SECURE_STORE_PATH}/truststore.jks
    sudo chmod 444 ${SECURE_STORE_PATH}/truststore.jks
fi
if [ -f ${ODIN_KEYSTORE_FILE} ] && [ -s ${ODIN_KEYSTORE_FILE} ]; then
    sudo cp ${ODIN_KEYSTORE_FILE} ${SECURE_STORE_PATH}/server.keystore
    sudo chmod 440 ${SECURE_STORE_PATH}/server.keystore
fi 

# update hostname placeholder in principal value in core/hdfs-site.xml, use system HOSTNAME env.
[ -f ${CORE_SITE} ] && sed -i "s|HOSTNAME|${HOSTNAME}|" ${CORE_SITE}
[ -f ${HDFS_SITE} ] && sed -i "s|HOSTNAME|${HOSTNAME}|" ${HDFS_SITE}

# set permissions for secrets
sudo chown -R udocker:udocker ${SECURE_STORE_PATH}
