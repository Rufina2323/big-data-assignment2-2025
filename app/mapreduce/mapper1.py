import re
import sys

# Iterate through each line of input from stdin
for line in sys.stdin:
    # Check that line is not empty
    if not line.strip():
        continue
    
    # Split the line into at most 3 parts using tab as the delimiter
    info = line.strip().split('\t', 2)
    if len(info) < 3:
        continue
    # Assign splitted parts to variables
    doc_id, doc_title, text = info

    # Use regular expression to extract all words (alphanumeric and apostrophes) in lowercase
    words = re.findall(r'[\w\']+', text.lower())

    # For each word print word and 1  
    for word in words:
        print(f"{word}\t1")
