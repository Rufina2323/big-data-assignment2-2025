#!/bin/bash
INPUT_PATH=${1:-/index/data}
OUTPUT_PATH="/tmp/index/output"

. ./.venv/bin/activate

if [[ "$INPUT_PATH" != "/index/data" ]]; then
  echo "Put file from $INPUT_PATH to hdfs"
  mkdir -p local_files
  OUTPUT_FILE="local_files/$(basename "$INPUT_PATH")"
  NUMBER_ID=$(( RANDOM % 900000 + 100000 ))
  TITLE=$(basename "$INPUT_PATH" .txt)
  TEXT=$(tr '\n' ' ' < "$INPUT_PATH")
  echo -e "${NUMBER_ID}\t${TITLE}\t${TEXT}" > "$OUTPUT_FILE"

  hdfs dfs -rm -r /local_files
  hdfs dfs -put local_files /
  hdfs dfs -ls /local_files

  INPUT_PATH="/local_files/$(basename "$INPUT_PATH")"

  rm -rf local_files
fi

# Step 1: Set up Cassandra schema
echo "Setting up Cassandra schema..."
cqlsh cassandra-server -f /app/cassandra/schema.cql

# Step 2: Run the MapReduce job
echo "Running MapReduce1 job..."
hadoop fs -rm -r $OUTPUT_PATH
echo "Running!"

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
  -archives /app/.venv.tar.gz#.venv \
  -mapper ".venv/bin/python mapper1.py" \
  -reducer ".venv/bin/python reducer1.py" \
  -input "$INPUT_PATH" \
  -output "$OUTPUT_PATH"

echo "Running MapReduce2 job..."
hadoop fs -rm -r $OUTPUT_PATH

echo "Running!"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
  -archives /app/.venv.tar.gz#.venv \
  -mapper ".venv/bin/python mapper2.py" \
  -reducer ".venv/bin/python reducer2.py" \
  -input "$INPUT_PATH" \
  -output "$OUTPUT_PATH"

echo "Indexing complete. Info inserted into Cassandra."

