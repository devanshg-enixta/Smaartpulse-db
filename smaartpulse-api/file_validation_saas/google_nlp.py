# coding: utf-8

from google.cloud.gapic.language.v1beta2 import language_service_client
from google.cloud.gapic.language.v1beta2 import enums
from google.cloud.proto.language.v1beta2 import language_service_pb2
import httplib2
import sys, os
from os import walk
from google.oauth2 import service_account

import pandas as pd
import re
import multiprocessing as mp
import Queue
import unicodecsv as csv
import time
import math
import random
import logging
from HTMLParser import HTMLParser

from utils.db_connection import get_engine
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

h = HTMLParser()

logger = logging.getLogger('flask.app')


def get_creds():
    credential_files = []
    keys_dir = os.path.join(BASE_DIR, 'config/google_keys')
    for (dirpath, dirnames, filenames) in walk(keys_dir):
        for filename in filenames:
            if filename.endswith('.json'):
                credential_files.append(filename)

    credentials_list = []
    for cf in credential_files:
        credentials = service_account.Credentials.from_service_account_file(os.path.join(keys_dir, cf))
        scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])
        try:
            client = language_service_client.LanguageServiceClient(credentials=scoped_credentials)
            document = language_service_pb2.Document(
                content="Working good",
                type=enums.Document.Type.PLAIN_TEXT)
            annotations = client.analyze_sentiment(document=document)
            credentials_list.append(scoped_credentials)
        except Exception as Ex:
            print "Failed to authenticate Key! ", cf
    return credentials_list


# Method to get the analysis of text from google
def analyze_df(content, creds):
    sentences = []
    try:
        """Run a sentiment analysis request on text within a passed filename."""
        client = language_service_client.LanguageServiceClient(credentials=creds)
        # content = row['review_text']

        document = language_service_pb2.Document(
            content=content,
            language="EN",
            type=enums.Document.Type.PLAIN_TEXT)
        annotations = client.analyze_sentiment(document=document)
        for index, sentence in enumerate(annotations.sentences):
            sentencee = sentence.text.content
            sentence_sentiment = sentence.sentiment.score
            sentence_magnitude = sentence.sentiment.magnitude
            # print '-------------',sentence,'----------------'
            try:
                nlps = {'sentence': sentencee, 'sentiment_score': sentence_sentiment,
                        'sentiment_magnitude': sentence_magnitude}
                sentences.append(nlps)
            except Exception as Ex:
                print "ignoring line", Ex
        return sentences
    except Exception as Ex:
        print "ignoring review", Ex
        return sentences


def gcloud_client(review_queue, output_queue, creds, direct=False):
    while True:
        try:
            reviews = review_queue.get(timeout=4)
            if not review_queue.qsize() % 10:
                print review_queue.qsize()
        except Exception as Ex:
            print "Queue is empty. Exiting!", Ex
            output_queue.put('STOP')
            break

        if reviews:
            content1 = []
            for review in reviews:
                content1.append(review['review_text'].strip())
            content = ' \n\n'.join(content1)

            if direct:
                cred = random.choice(creds)
            else:
                cred = creds
            output = analyze_df(content, cred)
            for sent in output:
                found = False
                for review in reviews:
                    gnlp_sentence = sent['sentence']
                    review_text = review['review_text']
                    gnlp_sentence = gnlp_sentence.decode('utf8') if not isinstance(gnlp_sentence, unicode) else unicode(gnlp_sentence)
                    gnlp_sentence = ''.join([y for y in gnlp_sentence.lower() if y.isalnum()])
                    review_text = ''.join([y for y in review_text.lower() if y.isalnum()])
                    if gnlp_sentence.strip().strip('.') in review_text:
                        found = True
                        mapped_sent = review.copy()
                        mapped_sent.update(sent)
                        output_queue.put(mapped_sent)
                if not found:
                    print "not found sentence in text"
                    print sent['sentence'],'\n'
                    print content,'\n\n\n\n'


def to_file(output_queue, processes, output_file):
    stop_count = 0
    outputfile = open(output_file, 'ab+')
    writer = csv.writer(outputfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
    while True:
        try:
            nlp_snippet = output_queue.get(timeout=4)
        except Queue.Empty:
            time.sleep(60)
            continue
        if nlp_snippet == 'STOP':
            print "Stop count", stop_count
            stop_count += 1
        else:
            writer.writerow([nlp_snippet['product_id'], nlp_snippet['review_id'],
                             nlp_snippet['sentence'], nlp_snippet['sentiment_score'],
                             nlp_snippet['sentiment_magnitude']])
        if stop_count == processes:
            outputfile.close()
            break


def punctuation_correction(text):
    text = text.lower()
    text = text.replace('...', '.').replace('..', '.').replace('\r\n', '\n') \
        .replace('\n\r', '\n').replace(".", ". ").replace(",", ", ") \
        .replace('\n\n', ' ').replace('\n', ' ').replace('\r\r', ' ') \
        .replace('\r', ' ').replace("  ", " ").replace("  ", " ").replace(' .','.')
    # text = text.replace('. and',' and').replace('.and',' and').replace('.or',' or').replace('. or',' or')
    text = text.strip()
    if not text[-1] in ('.','?','!'):
        text = text+'.'
    # text = h.unescape(re.sub(re.compile('([,.?! ]+$)'), '.', text))
    text = h.unescape(text)
    return text


def df_unicode(row):
    row = dict(row)
    for key, value in row.items():
        if isinstance(value, (str, unicode)):
            row[key] = value.decode('utf8') if not isinstance(value, unicode) else unicode(value)
    return row


def google_nlp(file_id, output_path='downloads/google'):
    output_file = os.path.join(output_path, '{}_google_nlp.csv'.format(file_id))
    cluster_size = 1000

    db = get_engine()
    reviews = pd.read_sql("select product_id, review_id, review_text from source_reviews "
                          "where file_id={} and (file_id, review_id) in "
                          "(select distinct file_id, review_id from sentiment_output_temp "
                          "where file_id={} and validate_status='Y');".format(file_id,file_id),con=db)
    reviews = reviews[['product_id', 'review_id', 'review_text']].drop_duplicates(['review_id','review_text'])
    reviews = pd.DataFrame(list(reviews.apply(df_unicode, axis=1)))
    if reviews.empty:
        raise(ValueError, "No Reviews found for file id {}".format(file_id))
    reviews.review_text = reviews.review_text.apply(punctuation_correction)

    try:
        previous_run = pd.read_csv(output_file, delimiter=',', quotechar='"',
                                   names=['product_id', 'review_id', 'sentence', 'sentiment_score',
                                          'sentiment_magnitude'])
        reviews = reviews[~reviews.review_id.isin(set(list(previous_run.review_id)))]
        del previous_run

    except:
        pass

    print "Review Count is ", reviews.shape[0]

    if not reviews.empty:
        credentials_list = get_creds()
        manager = mp.Manager()
        review_queue = manager.Queue()

        for product_id in set(list(reviews.product_id)):
            review_texts = reviews[reviews.product_id == product_id].to_dict('records')
            row_count = len(review_texts)
            clusters = (row_count/cluster_size) if (row_count % cluster_size == 0) else (row_count/cluster_size + 1)
            for cl in range(clusters):
                cluster_range1 = cl*cluster_size
                cluster_range2 =  min([(cl+1)*cluster_size, row_count])
                cluster  = review_texts[cluster_range1:cluster_range2]
                review_queue.put(cluster)

        output_queue = manager.Queue()

        # Start all the scraping processes
        write_process = mp.Process(target=to_file, args=(output_queue, 1, output_file))
        write_process.start()

        gcloud_client(review_queue, output_queue, credentials_list, direct=True)

        # Join all the scraping processes after completion
        write_process.join()
        write_process.terminate()

    logger.info("Completed Extracting google nlp snippets for file_id {}".format(file_id))

    nlp_snippets = pd.read_csv(output_file, sep=',', quotechar='"', quoting=csv.QUOTE_ALL,
                               names=['product_id', 'review_id', 'sentence', 'sentiment_score',
                                      'sentiment_magnitude']).drop_duplicates()
    nlp_snippets = nlp_snippets[~(nlp_snippets.sentence.isnull())]
    sent = []
    for row in nlp_snippets.to_dict("records"):
        if float(row["sentiment_score"]) > 0.0:
            row["sentiment"] = "positive"
        elif float(row["sentiment_score"]) < -0.25:
            row["sentiment"] = "negative"
        else:
            row["sentiment"] = "neutral"
        sent.append(row)
    nlp_snippets = pd.DataFrame.from_dict(sent)
    del sent
    enx_snippets = pd.read_sql("select id, review_id, sentiment_text, sentiment_type, confidence_score "
                               "from sentiment_output_temp "
                               "where file_id={} and validate_status='Y';".format(file_id), con=db)

    review_ids = list(enx_snippets.review_id.unique())
    enx_snippets['sent_text_cleaned'] = enx_snippets.sentiment_text.apply(
                                            lambda x: ''.join([y for y in x.lower() if y.isalnum()]))
    enx_snippets_dict = dict()
    for row in enx_snippets.to_dict('records'):
        review_id = row['review_id']
        if not review_id in enx_snippets_dict:
            enx_snippets_dict[review_id] = list()
        enx_snippets_dict[review_id].append(row)
    del enx_snippets

    nlp_snippets['sent_text_cleaned'] = nlp_snippets.sentence.apply(
                                            lambda x: ''.join([y for y in x.lower() if y.isalnum()]))
    nlp_snippets_dict = dict()
    for row in nlp_snippets.to_dict('records'):
        review_id = row['review_id']
        if not review_id in nlp_snippets_dict:
            nlp_snippets_dict[review_id] = list()
            nlp_snippets_dict[review_id].append(row)
    del nlp_snippets

    match = []
    non_match = []
    print "Started validation against google nlp output!"
    for review_id in review_ids:
        reviews_google = nlp_snippets_dict[review_id]
        reviews_enixta = enx_snippets_dict[review_id]
        for row1 in reviews_enixta:
            row_id = row1['id']
            found = False
            for row2 in reviews_google:
                if row1['sent_text_cleaned'] in row2['sent_text_cleaned'] \
                        or row2['sent_text_cleaned'] in row1['sent_text_cleaned']:
                    found = True
                    row1['sentiment_type'] = row1['sentiment_type'].strip()
                    row1['sentiment_type'] = row1['sentiment_type'].replace('most-positive', 'positive')
                    row1['sentiment_type'] = row1['sentiment_type'].replace('most-negative', 'negative')
                    if row2['sentiment'] == row1['sentiment_type'] or row1['confidence_score'] > 0.95:
                        match.append(row_id)
                    else:
                        non_match.append(row_id)
                    break
            if not found:
                if row1['confidence_score'] > 0.95:
                    match.append(row_id)
                else:
                    non_match.append(row_id)
    non_match_ids = ', '.join(map(str, non_match))
    print "completed google nlp comparision"
    if non_match_ids:
        query = "update sentiment_output_temp set validate_status='N', validate_comment='Failed Google NLP Validation' " \
                "where id in ({})".format(non_match_ids)
        db.execute(query)

    db.dispose()
    return True
