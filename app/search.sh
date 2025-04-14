#!/bin/bash
echo "This script will include commands to search for documents given the query using Spark RDD"

# Delete output file for query
hdfs dfs -rm -r /bm25_output

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit \
  --master yarn \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
  --archives /app/.venv.tar.gz#.venv \
  query.py "$1"

# Show obtained results for query
hdfs dfs -cat /bm25_output/*.csv