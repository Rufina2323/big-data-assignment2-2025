#!/bin/bash

# Set the input path to the first argument, or default to /index/data if not provided
INPUT_PATH=${1:-/index/data}
OUTPUT_PATH="/tmp/index/output"

# Activate Python virtual environment
. ./.venv/bin/activate

# If the input path is not the default, prepare the file for HDFS
if [[ "$INPUT_PATH" != "/index/data" ]]; then
  echo "Put file from $INPUT_PATH to hdfs"

  # Create a temporary local directory to hold the file
  mkdir -p local_files

  # Define the local output file path based on the original filename
  OUTPUT_FILE="local_files/$(basename "$INPUT_PATH")"

  # Generate a random 6-digit number for document ID
  NUMBER_ID=$(( RANDOM % 900000 + 100000 ))

  # Extract the document title from the filename (without extension)
  TITLE=$(basename "$INPUT_PATH" .txt)

  # Read the text from the input file
  TEXT=$(tr '\n' ' ' < "$INPUT_PATH")

  # Write to the document ID, title, text
  echo -e "${NUMBER_ID}\t${TITLE}\t${TEXT}" > "$OUTPUT_FILE"

  # Remove any existing /local_files folder in HDFS and upload the new one
  hdfs dfs -rm -r /local_files
  hdfs dfs -put local_files /
  hdfs dfs -ls /local_files

  INPUT_PATH="/local_files/$(basename "$INPUT_PATH")"

  # Delete a temporary local directory
  rm -rf local_files
fi

# Set up Cassandra schema
echo "Setting up Cassandra schema..."
cqlsh cassandra-server -f /app/cassandra/schema.cql

# Run the MapReduce job
echo "Running MapReduce1 job..."
hadoop fs -rm -r $OUTPUT_PATH
echo "Running!"

# Execute the MapReduce job using Python mapper and reducer (with virtualenv)
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
  -archives /app/.venv.tar.gz#.venv \
  -mapper ".venv/bin/python mapper1.py" \
  -reducer ".venv/bin/python reducer1.py" \
  -input "$INPUT_PATH" \
  -output "$OUTPUT_PATH"

# Run next MapReduce job
echo "Running MapReduce2 job..."
hadoop fs -rm -r $OUTPUT_PATH
echo "Running!"

# Execute the MapReduce job using Python mapper and reducer (with virtualenv)
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
  -archives /app/.venv.tar.gz#.venv \
  -mapper ".venv/bin/python mapper2.py" \
  -reducer ".venv/bin/python reducer2.py" \
  -input "$INPUT_PATH" \
  -output "$OUTPUT_PATH"

echo "Indexing complete. Info inserted into Cassandra."

