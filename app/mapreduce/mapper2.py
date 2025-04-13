import re
import sys

for line in sys.stdin:
    if not line.strip():
        continue
    
    info = line.strip().split('\t', 2)
    if len(info) < 3:
        continue
    doc_id, doc_title, text = info
    words = re.findall(r'[\w\']+', text.lower())
    for word in words:
        print(f"{doc_id}\t{doc_title}\t{word}\t1")
