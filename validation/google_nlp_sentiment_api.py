# coding: utf-8

from google.cloud.gapic.language.v1beta2 import language_service_client
from google.cloud.gapic.language.v1beta2 import enums
from google.cloud.proto.language.v1beta2 import language_service_pb2
import sys
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
from HTMLParser import HTMLParser
h = HTMLParser()


def get_creds():
    credential_files = []
    for (dirpath, dirnames, filenames) in walk('./google_keys/'):
        for filename in filenames:
            if filename.endswith('.json'):
                credential_files.append(filename)

    credentials_list = []
    for cf in credential_files:
        credentials = service_account.Credentials.from_service_account_file('google_keys/' + cf)
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
            writer.writerow([nlp_snippet['source_product_id'], nlp_snippet['source_review_id'],
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


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print "Three inputs are needed 1.filepath 2.delimiter 3.outputfile. "
        sys.exit(0)
    file_name = sys.argv[1]
    delimiter = sys.argv[2]
    output_file = sys.argv[3]

    cluster_size = 1000

    # db = MySQLdb.connect(host='localhost', port=3306, user='root', passwd='enixta@123', db='pipevendb')
    if file_name.endswith('.xlsx'):
        reviews = pd.read_excel(file_name, encoding='iso-8859-1')
    else:
        reviews = pd.read_csv(file_name, delimiter=delimiter, quotechar='"')
    reviews = reviews[['source_product_id', 'source_review_id', 'review_text']].drop_duplicates(['source_review_id','review_text'])
    reviews = pd.DataFrame(list(reviews.apply(df_unicode, axis=1)))
    reviews.review_text = reviews.review_text.apply(punctuation_correction)

    credentials_list = get_creds()
    try:
        previous_run = pd.read_csv(output_file, delimiter=',', quotechar='"',
                                   names=['source_product_id', 'source_review_id', 'sentence', 'sentiment_score',
                                          'sentiment_magnitude'])
        reviews = reviews[~reviews.source_review_id.isin(set(list(previous_run.source_review_id)))]
        del previous_run

    except:
        pass

    # processes = 2 * len(credentials_list)
    print "Review Count is ", reviews.shape[0]

    manager = mp.Manager()
    review_queue = manager.Queue()

    for source_product_id in set(list(reviews.source_product_id)):
        review_texts = reviews[reviews.source_product_id == source_product_id].to_dict('records')
        row_count = len(review_texts)
        clusters = (row_count/cluster_size) if (row_count % cluster_size == 0) else (row_count/cluster_size + 1)
        for cl in range(clusters):
            cluster_range1 = cl*cluster_size
            cluster_range2 =  min([(cl+1)*cluster_size, row_count])
            cluster  = review_texts[cluster_range1:cluster_range2]
            review_queue.put(cluster)

    output_queue = manager.Queue()
    # process_list = [mp.Process(target=gcloud_client,
    #                            args=(review_queue, output_queue, credentials_list[(processes - x) % int(math.ceil((processes / 2.0)))]))
    #                 for x in range(processes)]

    # write_process = mp.Process(target=to_file, args=(output_queue, processes, output_file))
    # write_process.start()

    # try:
    #     # Start all the scraping processes
    #     for process in process_list:
    #         process.start()

    #     # Join all the scraping processes after completion
    #     for process in process_list:
    #         process.join()
    #     write_process.join()
    #     print "Completed!"

    # except (KeyboardInterrupt, SystemExit):
    #     print "Got Interrupt. Killing the processes!"
    #     for process in process_list:
    #         process.terminate()
    #     write_process.terminate()

    # process_list = [mp.Process(target=gcloud_client,
    #                            args=(review_queue, output_queue, credentials_list[(processes - x) % int(math.ceil((processes / 2.0)))]))
    #                 for x in range(processes)]

    try:
        # Start all the scraping processes
        write_process = mp.Process(target=to_file, args=(output_queue, 1, output_file))
        write_process.start()

        # for process in process_list:
        #     process.start()

        gcloud_client(review_queue, output_queue, credentials_list, direct=True)

        # Join all the scraping processes after completion
        write_process.join()
        print "Completed!"

    except (KeyboardInterrupt, SystemExit):
        print "Got Interrupt. Killing the processes!"
        # for process in process_list:
        #     process.terminate()
        write_process.terminate()
