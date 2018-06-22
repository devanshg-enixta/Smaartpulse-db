import pandas as pd
import argparse
import time
import logging
import csv


def remove_bad_reviews_func(input_file,logging_filename,output_file,delimiter):
    # Set up a file handler for the log
    handler = logging.FileHandler(logging_filename, mode='w')
    FORMAT = '%(asctime)s : %(levelname)s : %(message)s'

    # Create the logger, add the file handler, and set the level to 'info'.
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    # Set the formatter in handler
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    # Start the timer
    start_time = time.time()
    
    # reading the input file into graph-lab SFrame
    #reviews = pd.read_csv(input_file, delimiter = delimiter, quoting=csv.QUOTE_ALL)
    reviews = pd.read_csv(input_file, delimiter = delimiter)

    # print "Before dropping duplicates"
    # print reviews.shape
    # print "After dropping duplicates"
    #reviews = reviews.drop_duplicates(subset=['source_product_id','review_text'])
    reviews = reviews.drop_duplicates(subset=['source_product_id','source_review_id'])

    # print reviews.shape
    logger.info('Removing those reviews which are duplicates in '
                'terms of source review id, source product id, review text')
    reviews['review_tag'] = ""
    reviews['review_text'] = reviews['review_text'].apply(lambda x:str(x).replace('\n', " ").replace('\t', " ").replace('\r', " ").replace("~"," ").strip())

    # print reviews['review_text'].head()
    reviews['review_text'] = reviews['review_text'].apply(lambda x: str(x).replace('###', '.'))

    

    # saving the DataFrame to output file
    reviews.to_csv(output_file, sep=delimiter,
               index=False,quoting=None)