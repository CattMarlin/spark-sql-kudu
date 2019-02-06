#!/bin/bash

# Get latest config on HDFS
hdfs dfs -put -f ../config/mviews.yml /database/config

# Run the app
spark2-submit \
--name spark-sql-kudu \
--master yarn \
--deploy-mode cluster \
--properties-file ../config/app.properties \
--class example.SparkSqlKudu \
../target/spark-sql-kudu-1.0-SNAPSHOT-jar-with-dependencies.jar

