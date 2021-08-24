#!/bin/bash
set -e
unset PYSPARK_DRIVER_PYTHON

# Beeline connection
beeline_connect="jdbc:hive2://sdpsvrwm0124.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0128.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0162.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0123.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0127.scglobal.ad.scotiacapital.com:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
bee="/usr/bin/beeline -u ${beeline_connect}"

echo "----------------------------------------------------"
echo "$(date) : Doing the job..."
echo "----------------------------------------------------"

currenttime=$(date +%H:%M)
dow=$(date +%u)
if (($dow<6)) && [[ "$currenttime" < "15:00" ]]; then NUMEXE=100; else NUMEXE=500; fi 

spark-submit \
--verbose \
--master yarn \
--deploy-mode client \
--driver-memory 10g \
--executor-memory 4g \
--files ./log4j.properties \
--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$(pwd)/log4j.properties \
--conf spark.driver.maxResultSize=1000g \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=$NUMEXE \
--conf spark.executor.heartbeatInterval=20 \
--jars build/libs/symcor_parse-shadow-1.0-all.jar src/main/main.py

echo "----------------------------------------------------"
echo "$(date) : Job Complete."
echo "----------------------------------------------------"