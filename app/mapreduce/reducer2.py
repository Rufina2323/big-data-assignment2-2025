import sys
from cassandra.cluster import Cluster

def get_cassandra_session():
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('index_keyspace')
    return session


def insert_word_frequency_in_doc(session, doc_id, doc_title, term, term_frequency, corpus_name="whole_corpus"):
    session.execute(
        "INSERT INTO term_frequency_in_doc (term, corpus_name, doc_id, doc_title, term_frequency) VALUES (%s, %s, %s, %s, %s)",
        (term, corpus_name, doc_id, doc_title, term_frequency)
    )


def insert_doc_info(session, doc_id, doc_title, doc_length, corpus_name="whole_corpus"):
    session.execute(
        "INSERT INTO doc_info (doc_id, doc_title, doc_length) VALUES (%s, %s, %s)",
        (doc_id, doc_title, doc_length)
    )


def insert_corpus_info(session, doc_n, total_doc_length, corpus_name="whole_corpus"):
    row = session.execute(
        "SELECT doc_n, total_doc_length FROM corpus_info WHERE corpus_name=%s",
        (corpus_name,)
    ).one()

    if row:
        new_doc_n = row.doc_n + doc_n
        new_total_doc_length = row.total_doc_length + total_doc_length
        session.execute(
            "UPDATE corpus_info SET doc_n=%s, total_doc_length=%s  WHERE corpus_name=%s",
            (new_doc_n, new_total_doc_length, corpus_name)
        )
    else:
        session.execute(
            "INSERT INTO corpus_info (corpus_name, doc_n, total_doc_length) VALUES (%s, %s, %s)",
            (corpus_name, doc_n, total_doc_length)
        )

word_frequency_in_doc = {}
doc_info = {}
for line in sys.stdin:
    doc_id, doc_title, word, count = line.split('\t')
    doc_id = int(doc_id.strip())
    doc_title = doc_title.strip()
    word = word.strip()
    count = int(count.strip())
    if (doc_id, doc_title, word) in word_frequency_in_doc:
        word_frequency_in_doc[(doc_id, doc_title, word)] += count
    else:
        word_frequency_in_doc[(doc_id, doc_title, word)] = count

    if (doc_id, doc_title) in doc_info:
        doc_info[(doc_id, doc_title)] += count
    else:
        doc_info[(doc_id, doc_title)] = count



session = get_cassandra_session()

for key, count in word_frequency_in_doc.items():
    doc_id, doc_title, word = key
    insert_word_frequency_in_doc(session, doc_id, doc_title, word, count)
    
total_doc_length = 0
doc_n = 0
for key, count in doc_info.items():
    doc_n += 1
    total_doc_length += count
    doc_id, doc_title = key
    insert_doc_info(session, doc_id, doc_title, count)

insert_corpus_info(session, doc_n, total_doc_length)
