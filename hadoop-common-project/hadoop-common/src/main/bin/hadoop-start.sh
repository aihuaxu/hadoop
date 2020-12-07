#!/usr/bin/env bash

# Runs a Hadoop component as a process in the foreground.
# This script is to be used in containerized deployments, where the process needs to run as the root process of the container.

usage="Usage: hadoop-start.sh <component>"

# if no args specified, show usage
if [ $# -eq 0 ]; then
  echo $usage
  exit 1
fi

envScript=$HADOOP_CONF_DIR/hadoop-env.sh
component=$1

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

DEFAULT_LIBEXEC_DIR="$bin"/../libexec
HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
. $HADOOP_LIBEXEC_DIR/hadoop-config.sh --env "$envScript"

hadoop_rotate_log ()
{
  log=$1;
  num=5;
  if [ -n "$2" ]; then
    num=$2
  fi
  if [ -f "$log" ]; then # rotate logs
	  while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	  done
	  mv "$log" "$log.$num";
  fi
}

# Determine if we're starting a secure datanode, and if so, redefine appropriate variables.
if [ "$component" == "datanode" ] && [ "$EUID" -eq 0 ] && [ -n "$HADOOP_SECURE_DN_USER" ]; then
  export HADOOP_LOG_DIR=$HADOOP_SECURE_DN_LOG_DIR
  export HADOOP_IDENT_STRING=$HADOOP_SECURE_DN_USER
fi

if [ "$HADOOP_IDENT_STRING" = "" ]; then
  export HADOOP_IDENT_STRING="$USER"
fi

if [ "$HADOOP_LOG_DIR" = "" ]; then
  export HADOOP_LOG_DIR="$HADOOP_PREFIX/logs"
fi

if [ ! -w "$HADOOP_LOG_DIR" ] ; then
  mkdir -p "$HADOOP_LOG_DIR"
  chown $HADOOP_IDENT_STRING $HADOOP_LOG_DIR
fi

export HADOOP_LOGFILE=hadoop-$HADOOP_IDENT_STRING-$component-$HOSTNAME.log
export HADOOP_ROOT_LOGGER=${HADOOP_ROOT_LOGGER:-"INFO,RFA"}
export HADOOP_SECURITY_LOGGER=${HADOOP_SECURITY_LOGGER:-"INFO,RFAS"}
export HDFS_AUDIT_LOGGER=${HDFS_AUDIT_LOGGER:-"INFO,NullAppender"}
log=$HADOOP_LOG_DIR/hadoop-$HADOOP_IDENT_STRING-$component-$HOSTNAME.out

hadoop_rotate_log $log
echo "starting $component, logging to $log"


case $component in
  (namenode|datanode|journalnode|zkfc|balancer|dfsrouter)
    hdfsScript="$HADOOP_HDFS_HOME"/bin/hdfs
    echo "Running command: exec $hdfsScript $@"
    exec $hdfsScript "$@" > "$log" 2>&1
  ;;
  (*)
    echo "$component should be one of namenode,datanode,journalnode,zkfc,balancer,dfsrouter"
    echo $usage
    exit 1
  ;;
