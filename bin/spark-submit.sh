#!/bin/bash

set -e

BINDIR=$(dirname $0)
cd $BINDIR/..

if [[ "$1" == "-h" ]]; then
  echo 'Usage: ./bin/spark-submit.sh <job name> [<job arguments> ...]'
  exit 1
fi

export PYSPARK_DRIVER_PYTHON=./environment/bin/python3
export PYSPARK_PYTHON=./environment/bin/python3

spark-submit --archives $BINDIR/pyspark_conda_env.tar.gz#environment\
  --conf spark.shuffle.service.enabled=true\
  --conf spark.dynamicAllocation.enabled=true\
  --conf spark.dynamicAllocation.minExecutors=2\
  --conf spark.dynamicAllocation.maxExecutors=2\
  --conf spark.dynamicAllocation.initialExecutors=2\
  --conf spark.executor.memory=1g\
  --conf spark.driver.memory=1g\
  --conf spark.yarn.executor.memoryOverhead=1g\
  --conf spark.driver.maxResultSize=1g\
  --conf spark.hadoop.hive.metastore.uris= \
  --master yarn\
  --deploy-mode cluster\
  --queue default\
  $BINDIR/sparkapp.py $@

exit $?
