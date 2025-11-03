#!/bin/bash
set -e

. "${SPARK_HOME}/bin/load-spark-env.sh"

if [ "$SPARK_MODE" = "master" ]; then
  echo "Starting Spark Master..."
  exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master \
    --host $(hostname) --port 7077 --webui-port 8080
elif [ "$SPARK_MODE" = "worker" ]; then
  echo "Starting Spark Worker..."
  exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port 8081 spark://$SPARK_MASTER_URL:7077
else
  echo "Unknown SPARK_MODE: $SPARK_MODE"
  exit 1
fi
