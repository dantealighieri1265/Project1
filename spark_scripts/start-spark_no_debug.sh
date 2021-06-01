#!/bin/bash
echo "Starting Spark ..."
$SPARK_HOME/bin/spark-submit --class "queries.ClassForTest" --master "local" ../target/SABD_Project1-0.0.1-SNAPSHOT-jar-with-dependencies.jar false