import sys
from cassandra.cluster import Cluster

# Function to establish a Cassandra session connected to the 'index_keyspace'
def get_cassandra_session():
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('index_keyspace')
    return session

# Function to insert the word frequency for document in Cassandra
def insert_word_frequency_in_doc(session, doc_id, doc_title, term, term_frequency, corpus_name="whole_corpus"):
    session.execute(
        "INSERT INTO term_frequency_in_doc (term, corpus_name, doc_id, doc_title, term_frequency) VALUES (%s, %s, %s, %s, %s)",
        (term, corpus_name, doc_id, doc_title, term_frequency)
    )

# Function to insert the document info in Cassandra
def insert_doc_info(session, doc_id, doc_title, doc_length, corpus_name="whole_corpus"):
    session.execute(
        "INSERT INTO doc_info (doc_id, doc_title, doc_length) VALUES (%s, %s, %s)",
        (doc_id, doc_title, doc_length)
    )

# Function to insert or update the corpus info in Cassandra
def insert_corpus_info(session, doc_n, total_doc_length, corpus_name="whole_corpus"):
    # Check if the corpus already exists
    row = session.execute(
        "SELECT doc_n, total_doc_length FROM corpus_info WHERE corpus_name=%s",
        (corpus_name,)
    ).one()

    if row:
        # If the corpus exists, update its total document number and total document length by adding the new counts
        new_doc_n = row.doc_n + doc_n
        new_total_doc_length = row.total_doc_length + total_doc_length
        session.execute(
            "UPDATE corpus_info SET doc_n=%s, total_doc_length=%s  WHERE corpus_name=%s",
            (new_doc_n, new_total_doc_length, corpus_name)
        )
    else:
        # If the corpus does not exist, insert a new row with the corpus name, total document number, and total document length
        session.execute(
            "INSERT INTO corpus_info (corpus_name, doc_n, total_doc_length) VALUES (%s, %s, %s)",
            (corpus_name, doc_n, total_doc_length)
        )


# Store word frequency for each document
word_frequency_in_doc = {}

# Store document info
doc_info = {}
for line in sys.stdin:
    doc_id, doc_title, word, count = line.split('\t')
    doc_id = int(doc_id.strip())
    doc_title = doc_title.strip()
    word = word.strip()
    count = int(count.strip())

    # Aggregate word frequency for document in the dictionary
    if (doc_id, doc_title, word) in word_frequency_in_doc:
        word_frequency_in_doc[(doc_id, doc_title, word)] += count
    else:
        word_frequency_in_doc[(doc_id, doc_title, word)] = count

    # Aggregate length of document in the dictionary
    if (doc_id, doc_title) in doc_info:
        doc_info[(doc_id, doc_title)] += count
    else:
        doc_info[(doc_id, doc_title)] = count

# Establish a Cassandra session
session = get_cassandra_session()

# Insert word frequency for each document in Cassandra
for key, count in word_frequency_in_doc.items():
    doc_id, doc_title, word = key
    insert_word_frequency_in_doc(session, doc_id, doc_title, word, count)

# Store info for corpus
total_doc_length = 0
doc_n = 0

# Insert information for each document in Cassandra
for key, count in doc_info.items():
    doc_n += 1
    total_doc_length += count
    doc_id, doc_title = key
    insert_doc_info(session, doc_id, doc_title, count)

# Insert or update corpus info in Cassandra
insert_corpus_info(session, doc_n, total_doc_length)
