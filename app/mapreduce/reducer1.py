import sys
from cassandra.cluster import Cluster

# Function to establish a Cassandra session connected to the 'index_keyspace'
def get_cassandra_session():
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('index_keyspace')
    return session

# Function to insert or update the document frequency of a term in Cassandra
def insert_word_frequency(session, term, doc_frequency, corpus_name="whole_corpus"):
    # Check if the term already exists in the table for the given corpus
    row = session.execute(
        "SELECT doc_frequency FROM doc_frequency_of_term WHERE corpus_name=%s AND term=%s",
        (corpus_name, term)
    ).one()

    if row:
        # If the term exists, update its document frequency by adding the new count
        new_frequency = row.doc_frequency + doc_frequency
        session.execute(
            "UPDATE doc_frequency_of_term SET doc_frequency=%s  WHERE corpus_name=%s AND term=%s",
            (new_frequency, corpus_name, term)
        )
    else:
        # If the term does not exist, insert a new row with the term, corpus, and frequency
        session.execute(
            "INSERT INTO doc_frequency_of_term (term, corpus_name, doc_frequency) VALUES (%s, %s, %s)",
            (term, corpus_name, doc_frequency)
        )

# Store word frequency from stdin
word_frequency = {}
for line in sys.stdin:
    word, count  = line.split('\t', 1)
    word = word.strip()
    count = int(count.strip())

    # Aggregate word frequency in the dictionary
    if word in word_frequency:
        word_frequency[word] += count
    else:
        word_frequency[word] = count

# Establish a Cassandra session
session = get_cassandra_session()

# Insert or update each document frequency for each word in Cassandra
for word, count in word_frequency.items():
    insert_word_frequency(session, word, count)
