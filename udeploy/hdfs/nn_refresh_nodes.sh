#!/bin/bash

echo 'Refreshing Nodes'

cp -r /shared/datanode_hosts/* /opt/hdfs/hosts/
chown -R udocker:udocker /opt/hdfs/hosts/
chmod -R 644 /opt/hdfs/hosts/*

sudo -u udocker bash -c 'source /opt/hdfs/conf/hadoop-env.sh && hdfs dfsadmin -refreshNodes'
