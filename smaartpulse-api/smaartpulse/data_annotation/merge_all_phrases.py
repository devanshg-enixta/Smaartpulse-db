import os
from gensim.utils import to_unicode, to_utf8

PHRASE_DIR = "fine_grained_phrases_tvs"
output_file = "valid_phrases.txt"

if __name__ == "__main__":

    outfile = open(output_file, 'wb')

    for root, dirs, files in os.walk(PHRASE_DIR):
        for file in filter(lambda file: file.endswith('.txt'), files):
            document = open(os.path.join(root, file), 'rb')

            for line in document:
                line = line.replace("_", " ")
                print >> outfile, line.strip()

            document.close()

    outfile.close()