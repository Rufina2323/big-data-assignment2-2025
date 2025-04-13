import sys
# from app import get_cassandra_session, insert_word_frequency
from cassandra.cluster import Cluster

def get_cassandra_session():
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('index_keyspace')
    return session

def insert_word_frequency(session, term, doc_frequency, corpus_name="whole_corpus"):
    row = session.execute(
        "SELECT doc_frequency FROM doc_frequency_of_term WHERE corpus_name=%s AND term=%s",
        (corpus_name, term)
    ).one()

    if row:
        new_frequency = row.doc_frequency + doc_frequency
        session.execute(
            "UPDATE doc_frequency_of_term SET doc_frequency=%s  WHERE corpus_name=%s AND term=%s",
            (new_frequency, corpus_name, term)
        )
    else:
        session.execute(
            "INSERT INTO doc_frequency_of_term (term, corpus_name, doc_frequency) VALUES (%s, %s, %s)",
            (term, corpus_name, doc_frequency)
        )


word_frequency = {}
for line in sys.stdin:
    word, count  = line.split('\t', 1)
    word = word.strip()
    count = int(count.strip())
    if word in word_frequency:
        word_frequency[word] += count
    else:
        word_frequency[word] = count


session = get_cassandra_session()

for word, count in word_frequency.items():
    insert_word_frequency(session, word, count)
