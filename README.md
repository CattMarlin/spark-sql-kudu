# Spark SQL Kudu Example
> Template to run SQL on Kudu tables and save to Hive

This example provides a YAML template & code base to run SQL on Kudu tables with Spark.  The YAML file is read from HDFS but can be configured to be read from the edge node.

## How to use

* Edit the queries.yml to include any queries you want to run.  List the Kudu tables.  Hive tables can be included in the query as well.
* Edit database location and Run the job with run-spark-sql-kudu.sh.  

