#!/bin/bash

set -e
set -x
set -o pipefail

# Exit Code Chart
#  
# 1 - system/env check failure
# 2 - ZKFC container failure
# 4 - Unknown container type

# Find current directory.
SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# start datanode process.
function start_datanode() {
  if [ "${PRIVILEGED_PORT}" == "true" ]; then
    echo 'INFO: Privileged port flag is set. Launching datanode as root with jsvc.'
    exec sudo /bin/bash -c "source ${HADOOP_ENV}; ${HDFS_START_SCRIPT} datanode"
  fi
  source "${HADOOP_ENV}"; exec "${HDFS_START_SCRIPT}" datanode
}

# start namenode process.
function start_namenode() {
  # INIT == true -> active NN in initialization and do the format.
  if [ "${INIT}" == "true" ]; then

    # Setup required directories through idempotent operations.
    if [ -z "${DYNAMIC_NAMENODE_NAME_DIR}" ]; then
      echo 'ERROR: env DYNAMIC_NAMENODE_NAME_DIR is empty.'
      exit 1
    fi
    if [ -z "${DYNAMIC_NAMENODE_LEGACY_OIV_DIR}" ]; then
      echo 'ERROR: env DYNAMIC_NAMENODE_LEGACY_OIV_DIR is empty.'
      exit 1
    fi
    namenode_name_dir_parent=$(dirname "${DYNAMIC_NAMENODE_NAME_DIR}")
    namenode_legacy_oiv_dir_parent=$(dirname "${DYNAMIC_NAMENODE_LEGACY_OIV_DIR}")
    sudo mkdir -p  "${DYNAMIC_NAMENODE_NAME_DIR}" "${DYNAMIC_NAMENODE_LEGACY_OIV_DIR}"
    sudo chmod -R 750 "${namenode_name_dir_parent}" "${namenode_legacy_oiv_dir_parent}"
    sudo chown -R udocker:udocker "${namenode_name_dir_parent}" "${namenode_legacy_oiv_dir_parent}"

    source "${HADOOP_ENV}"; "${HDFS_START_SCRIPT}" namenode -format ${init_option}
    if [ $? -ne 0 ]; then
      echo 'ERROR: Failed to format namenode.'
      # Not exit on error as it is expected if NN is already formatted.
    else
      echo 'INFO: Successfully formatted namenode.'
    fi
  # INIT not set -> standby NN in initialization,
  # INIT == false -> maybe a replacement standby NN in running cluster.
  else
    source "${HADOOP_ENV}"; "${HDFS_START_SCRIPT}" namenode -bootstrapStandby ${init_option}
    if [ $? -ne 0 ]; then
      echo 'ERROR: Failed to bootstrap standby namenode.'
      # Not exit on error as it is expected for NN restart.
    else
      echo 'INFO: Successfully bootstrap standby namenode.'
    fi
  fi

  sudo service cron start
  # Observer namenode requires -observer flag to start.
  [[ "${container_type}" == "ob" ]] && observer_opt="-observer" || observer_opt=""
  source "${HADOOP_ENV}"; exec "${HDFS_START_SCRIPT}" namenode ${observer_opt}
}

# start journalnode process.
function start_journalnode() {
  source "${HADOOP_ENV}"; exec "${HDFS_START_SCRIPT}" journalnode
}

# start zkfc process.
function start_zkfc() {
  if [ "${INIT}" == "true" ]; then
    source "${HADOOP_ENV}"; "${HDFS_START_SCRIPT}" zkfc -formatZK ${init_option}
    if [ $? -ne 0 ]; then
      echo 'ERROR: Failed to format ZKFC.'
      # Not exit on error as it is expected if znode already exist.
    else
      echo 'INFO: Successfully initialized ZK entry.'
    fi
  fi

  source "${HADOOP_ENV}"; exec "${HDFS_START_SCRIPT}" zkfc
}

# start balancer process.
function start_balancer() {
  source "${HADOOP_ENV}"; exec "${HDFS_CMD}" balancer -sortTopNodes -threshold "${BALANCER_THRESHOLD:-3}" 2>&1
}

# setup system files/dirs.
function setup_system() {
  chmod 755 /run
  mkdir -p /var/run/hdfs-sockets
  chown udocker:udocker /var/run/hdfs-sockets
  chmod 755 /var/run/hdfs-sockets
}

# entry point for this script.
function main() {
  source "${SCRIPTS_DIR}/env.sh"
  setup_system
  bash "${SCRIPTS_DIR}/config.sh"

  case ${container_type} in
    nn | ob)
      start_namenode
      ;;
    dn)
      start_datanode
      ;;
    jn)
      start_journalnode
      ;;
    zkfc)
      start_zkfc
      ;;
    balancer)
      start_balancer
      ;;
    *)
      echo "ERROR: Unknown HDFS service container type: ${container_type}"
      exit 4
      ;;
  esac
}

#############
#
# Main
#
#############

# Prepare variables that will be used in later steps.
container_type=$( echo ${CONTAINER_TYPE} | tr '[:upper:]' '[:lower:]' )
# Add the FORCE_FORMAT env just in case the need to force format.
[[ "${FORCE_FORMAT}" == "true" ]] && init_option="-force -nonInteractive" || init_option="-nonInteractive" 

main

if [ $? -ne 0 ]; then
  echo "ERROR: Failed to start ${container_type}."
  exit 1
fi
