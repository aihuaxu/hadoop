#!/bin/bash

set -e
set -o pipefail

mkdir -p ${HDFS_CONF_PATH}


# Copy over dsc configs to target path.
config_hdfs_conf() {
  bash ${DSC_SETUP_SCRIPT} ${DSC_TARGET_DIR}
  rsync --exclude *.sha -av ${DSC_TARGET_DIR}/${DSC_SUB_PATH}/* ${HDFS_CONF_PATH}/
  cp ./udeploy/hdfs/topology.py ${HDFS_CONF_PATH}/
  cp -r ./udeploy/hdfs/jsvc ${HDFS_CONF_PATH}/jsvc
}

# Populate ssl secrets, setup principal and keytabs.
config_secrets() {
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

  # fetch langley secrets from files landed by odin-hdfs-worker
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
  [ -f ${CORE_SITE} ] && sed -i "s|HOSTNAME|${HOSTNAME}|g" ${CORE_SITE}
  [ -f ${HDFS_SITE} ] && sed -i "s|HOSTNAME|${HOSTNAME}|g" ${HDFS_SITE}

  # set permissions for secrets
  sudo chown -R udocker:udocker ${SECURE_STORE_PATH}

  # set permissions for data dirs
  # secure cluster should have HDFS_KEYTAB_PATH exported by Odin HDFS worker.
  if [ -f "${HDFS_KEYTAB_PATH}" ]; then
    for dir in $(ls / | grep data[0-9][0-9]); do
      target_dir="/${dir}/dfs"
      if [ -d "${target_dir}" ]; then
        {
          sudo chmod 700 ${target_dir}
        } || {
          # by pass error, see https://t3.uberinternal.com/browse/HDFS-619
          echo "bad data dir ${target_dir}"
        }
      fi
    done
  fi
}

setup_production_kdc_env() {
    if [[ -z $UBER_REGION ]]; then
        echo 'ERROR: Env UBER_REGION not set' && exit 1
    fi

    export KRB_KDC_REALM='DATASRE.PROD.UBER.INTERNAL'
    export KRB_KDC_HOST_ADDR="hadoopkdc-${UBER_REGION}.uber.internal"
    export KRB_ADMIN_SERVER_HOST_ADDR='hadoopldap00-phx2.prod.uber.internal'
    export KRB_DOMAIN_REALM='prod.uber.internal'
}

setup_staging_kdc_env() {
    export KRB_KDC_REALM='DATA.STAGING.UBER.INTERNAL'
    export KRB_KDC_HOST_ADDR="hadoopkdcstaging-phx.uber.internal"
    export KRB_ADMIN_SERVER_HOST_ADDR='hadoopkdcstaging01-phx2.prod.uber.internal'
    export KRB_DOMAIN_REALM='prod.uber.internal'
}

config_krb5_conf() {
  if [[ $UBER_RUNTIME_ENVIRONMENT == 'production' ]]; then
      setup_production_kdc_env
  else
      [[ $UBER_RUNTIME_ENVIRONMENT == 'staging' ]]
      setup_staging_kdc_env
  fi
  
  sudo rm -f "${KRB5_CONF_PATH}"
  dir=$(dirname "$0")
  sudo install -o udocker -g udocker -m 666 /dev/null "${KRB5_CONF_PATH}"
  envsubst < "${dir}/${KRB5_CONF_TEMPLATE}" > "${KRB5_CONF_PATH}"
  sudo chmod 644 "${KRB5_CONF_PATH}"
}

config_hdfs_conf
config_secrets
config_krb5_conf
