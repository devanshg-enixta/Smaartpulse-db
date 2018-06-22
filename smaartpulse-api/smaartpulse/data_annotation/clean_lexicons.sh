#!/bin/bash

lexicons_dir="fine_grained_phrases_tvs/"
temp_file="temp2.txt"

rm ${temp_file} 2>/dev/null

for lexicon_file in $(find ${lexicons_dir} -name '*.txt' -type f | sort)
do
    echo ${lexicon_file}

    # Replacing the '_' by ' '
    sed -i.bak "s/_/ /g" ${lexicon_file}

    # Sorting the lexicon file in alphabetical order and selecting unique lexicons
    sort ${lexicon_file} | uniq > ${temp_file}

    # Moving the temp file to lexicon file
    mv ${temp_file} ${lexicon_file}

    # Removing the new line at the end of file
    sed -i.bak '/^$/d' ${lexicon_file}
done