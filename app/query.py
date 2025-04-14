import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, lit, log
import sys
import math
from functools import reduce

# Obtain query
query = sys.argv

# Initialize Spark session and configure Cassandra connection
spark = SparkSession.builder \
    .appName('BM25Ranking') \
    .config("spark.cassandra.connection.host", "cassandra-server") \
    .getOrCreate()

# Extract unique and lowercased query terms
query_terms = list(set([word.lower() for word in query[1].split()]))

corpus_name = 'whole_corpus'

# Build a list of OR-based conditions to query Cassandra for relevant terms using partition keys
conditions_list = [f"(corpus_name = '{corpus_name}' AND term = '{term}')" for term in query_terms]
conditions = " OR ".join(conditions_list)
where_clause = f"{conditions}"

# Load document frequency data for query terms from Cassandra
vocab_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="doc_frequency_of_term", keyspace="index_keyspace") \
    .load() \
    .where(where_clause)

# If no matching vocabulary terms found in the corpus, create and write an empty result
if not vocab_df.head(1):
    # Define schema
    schema = StructType([
        StructField("doc_id", IntegerType(), True),
        StructField("doc_title", StringType(), True),
        StructField("doc_rank", FloatType(), True),
    ])

    # Create an empty DataFrame with the defined schema
    empty_df = spark.createDataFrame([], schema)

    # Write the empty DataFrame
    empty_df.write.csv("/bm25_output", sep = "\t")

    # Stop the Spark session to finish the program
    spark.stop()
    sys.exit(0)


# Retrieve all matching vocabulary rows into driver memory
rows = vocab_df.collect()
dfs = []

docs = set()

# For each query term found in the vocab:
for row in rows:
    condition = f"corpus_name = '{row['corpus_name']}' AND term = '{row['term']}'"
    
    # Load corresponding term frequency data from Cassandra
    tf_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="term_frequency_in_doc", keyspace="index_keyspace") \
        .load() \
        .where(condition)
    dfs.append(tf_df)

    # Extract unique doc_ids from term frequency rows
    for tf_row in tf_df.select("doc_id", "doc_title").collect():
        docs.add(tf_row["doc_id"])

# Merge all term frequency DataFrames into one
if len(dfs) > 1:
    merged_tf_df = reduce(lambda a, b: a.unionByName(b), dfs)
else:
    merged_tf_df = dfs[0]

docs = list(docs)

docs_df = []

# For each relevant doc_id, extract document metadata from Cassandra
for doc in docs:
    condition = f"doc_id = {doc}"
    doc_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="doc_info", keyspace="index_keyspace") \
        .load() \
        .where(condition)
    docs_df.append(doc_df)

# Merge all doc_info DataFrames into one
if len(docs_df) > 1:
    merged_docs_df = reduce(lambda a, b: a.unionByName(b), docs_df)
else:
    merged_docs_df = docs_df[0]

# Join term frequency data with document frequency data (by term + corpus)
joined_df = merged_tf_df.join(vocab_df, on=["term", "corpus_name"])

# Join the result with document info (by doc_id + title)
joined_df = joined_df.join(merged_docs_df, on=["doc_id", "doc_title"])


total_docs = 0
avg_doc_len = 0

# Extract from the Cassandra 'corpus_info' table
condition = f"corpus_name = '{corpus_name}'"
corpus_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="corpus_info", keyspace="index_keyspace") \
    .load() \
    .where(condition)

rows = corpus_df.collect()

# Store number of documents in corpus and calculate avg document length
for row in rows:
    total_docs = row["doc_n"]
    avg_doc_len = float(row["total_doc_length"] / row["doc_n"])

# Hyperparameters for bm25 score
k1 = 1
b = 0.75

# Calculate bm25 score for query word and document pair
joined_df = joined_df.withColumn("bm25", log(lit(total_docs) / col("doc_frequency")) * ((k1 + 1) * col("term_frequency")) / (k1 * (1 - b + b * col("doc_length") / avg_doc_len) + col("term_frequency")))


# Aggregate BM25 per document using sum
ranked_docs = joined_df.groupBy("doc_id", "doc_title").sum("bm25") \
    .withColumnRenamed("sum(bm25)", "doc_rank")

# Sort results and take only top 10
ranked_docs = ranked_docs.orderBy(col("doc_rank").desc()).limit(10)
ranked_docs.write.csv("/bm25_output", sep = "\t")
