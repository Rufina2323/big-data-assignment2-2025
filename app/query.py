import pyspark
import sys
# from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, lit, log
import sys
import math
from functools import reduce


query = sys.argv

spark = SparkSession.builder \
    .appName('BM25Ranking') \
    .config("spark.cassandra.connection.host", "cassandra-server") \
    .getOrCreate()

query_terms = list(set([word.lower() for word in query[1].split()]))

corpus_name = 'whole_corpus'

conditions_list = [f"(corpus_name = '{corpus_name}' AND term = '{term}')" for term in query_terms]

conditions = " OR ".join(conditions_list)
where_clause = f"{conditions}"

vocab_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="doc_frequency_of_term", keyspace="index_keyspace") \
    .load() \
    .where(where_clause)

# TODO: rewrite
if not vocab_df.head(1):
    # Define schema (this could be empty or have some fields)
    schema = StructType([
        StructField("doc_id", IntegerType(), True),
        StructField("doc_title", StringType(), True),
        StructField("doc_rank", FloatType(), True),
    ])

    # Create an empty DataFrame with the defined schema
    empty_df = spark.createDataFrame([], schema)

    # Write the empty DataFrame to a location (e.g., CSV)
    empty_df.write.csv("/bm25_output", sep = "\t")

    # Stop the Spark session to finish the program
    spark.stop()

    sys.exit(0)



rows = vocab_df.collect()  # Bring all rows to driver
dfs = []

docs = set()

for row in rows:
    condition = f"corpus_name = '{row['corpus_name']}' AND term = '{row['term']}'"
    tf_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="term_frequency_in_doc", keyspace="index_keyspace") \
        .load() \
        .where(condition)
    dfs.append(tf_df)

    # Bring rows to driver and extract doc_id/doc_title
    for tf_row in tf_df.select("doc_id", "doc_title").collect():
        docs.add(tf_row["doc_id"])
    
if len(dfs) > 1:
    merged_tf_df = reduce(lambda a, b: a.unionByName(b), dfs)
else:
    merged_tf_df = dfs[0]



docs = list(docs)

docs_df = []
for doc in docs:
    condition = f"doc_id = {doc}"
    doc_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="doc_info", keyspace="index_keyspace") \
        .load() \
        .where(condition)
    docs_df.append(doc_df)

if len(docs_df) > 1:
    merged_docs_df = reduce(lambda a, b: a.unionByName(b), docs_df)
else:
    merged_docs_df = docs_df[0]


joined_df = merged_tf_df.join(vocab_df, on=["term", "corpus_name"])

joined_df = joined_df.join(merged_docs_df, on=["doc_id", "doc_title"])


total_docs = 0
avg_doc_len = 0

condition = f"corpus_name = '{corpus_name}'"
corpus_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="corpus_info", keyspace="index_keyspace") \
    .load() \
    .where(condition)

rows = corpus_df.collect()

for row in rows:
    total_docs = row["doc_n"]
    avg_doc_len = float(row["total_doc_length"] / row["doc_n"])


k1 = 1
b = 0.75


joined_df = joined_df.withColumn("bm25", log(lit(total_docs) / col("doc_frequency")) * ((k1 + 1) * col("term_frequency")) / (k1 * (1 - b + b * col("doc_length") / avg_doc_len) + col("term_frequency")))


# # 5. Aggregate BM25 per document
ranked_docs = joined_df.groupBy("doc_id", "doc_title").sum("bm25") \
    .withColumnRenamed("sum(bm25)", "doc_rank")

# # 6. Optional: Sort results
ranked_docs = ranked_docs.orderBy(col("doc_rank").desc()).limit(10)
ranked_docs.write.csv("/bm25_output", sep = "\t")
