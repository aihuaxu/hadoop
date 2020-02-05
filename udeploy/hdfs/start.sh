#!/bin/bash

# Exit Code Chart
#  
# 1 - system checking failure
# 2 - ZKFC container failure
# 4 - Unknown container type


# start datanode process.
function start_datanode() {
  source /opt/hdfs/conf/hadoop-env.sh; exec hdfs datanode
}

# start namenode process.
function start_namenode() {
  # INIT == true -> active NN in initialization and do the format.
  if [ "${INIT}" == "true" ]; then
    source /opt/hdfs/conf/hadoop-env.sh; exec hdfs namenode -format ${init_option}
    if [ $? -ne 0 ]; then
      echo "ERROR: Failed to format namenode."
      # Not exit on error as it is expected if NN is already formatted.
    else
      echo "INFO: Successfully formatted namenode."
    fi
  # INIT not set -> standby NN in initialization,
  # INIT == false -> maybe a replacement standby NN in running cluster.
  else
    source /opt/hdfs/conf/hadoop-env.sh; exec hdfs namenode -bootstrapStandby ${init_option}
    if [ $? -ne 0 ]; then
      echo "ERROR: Failed to bootstrap standby namenode."
      # Not exit on error as it is expected for NN restart.
    else
      echo "INFO: Successfully bootstrap standby namenode."
    fi
  fi

  sudo service cron start
  source /opt/hdfs/conf/hadoop-env.sh; exec hdfs namenode
}

# start journalnode process.
function start_journalnode() {
  source /opt/hdfs/conf/hadoop-env.sh; exec hdfs journalnode
}

# start zkfc process.
function start_zkfc() {
  if [ "${INIT}" == "true" ]; then
    source /opt/hdfs/conf/hadoop-env.sh; exec hdfs zkfc -formatZK ${init_option}
    if [ $? -ne 0 ]; then
      echo "ERROR: Failed to format ZKFC."
      # Not exit on error as it is expected if znode already exist.
    else
      echo "INFO: Successfully initialized ZK entry."
    fi
  fi

  source /opt/hdfs/conf/hadoop-env.sh; exec hdfs zkfc
}

# check if container type is valid.
function check_env() {
  # check container type.
  if [ -z "${container_type}" ]; then
    echo "ERROR: env CONTAINER_TYPE is empty."
    exit 1
  fi
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
  check_env
  setup_system
  bash config.sh

  # Not consider router and observer for now.
  case ${container_type} in
    nn)
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
