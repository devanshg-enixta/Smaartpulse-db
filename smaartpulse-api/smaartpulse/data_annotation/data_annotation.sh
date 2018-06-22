#!/bin/bash

PRESENT_DIR=$1

# Directory which contains the curated phrases files for creating supervised data
PHRASE_DIR=$2

# File having words which induce negations
NEG_WORDS_FILE_PATH=$3

#REVIEWS_FILE=$5"temp.txt"
REVIEWS_FILE=$4

#OUTPUT_FILE=$5"temp_results.txt"
OUTPUT_FILE=$5

logging_dir=$6
logging_filename=${logging_dir}"data_annotation.log"

master_logging_file=$7

WINDOW=16
REVIEW_FIELD=9
DELIMITER="~"

echo | tee -a ${master_logging_file}
echo "Run the script fine_grained_sentiment_search.py to annotate data using curated keywords"\
 | tee -a ${master_logging_file}



python ${PRESENT_DIR}fine_grained_sentiment_search_NEW.py --window ${WINDOW} --output_file ${OUTPUT_FILE} \
--reviews_file ${REVIEWS_FILE} --logging_filename ${logging_filename} --phrase_dir ${PHRASE_DIR} \
--review_field ${REVIEW_FIELD} --delimiter ${DELIMITER} --negations_file ${NEG_WORDS_FILE_PATH}
