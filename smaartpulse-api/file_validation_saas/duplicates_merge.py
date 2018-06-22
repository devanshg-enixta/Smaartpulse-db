# -*- encoding: utf-8 -*-
__author__ = 'Sreenivasa Rao Pallapu'

import pandas as pd
import re
import nltk
from collections import OrderedDict
import unicodecsv as csv
import datetime
import sys, os
import multiprocessing as mp
import Queue
import time
from HTMLParser import HTMLParser
from utils.db_connection import get_engine
from sqlalchemy.sql import text

h = HTMLParser()

stop_words = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself',
              'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself',
              'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that',
              'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
              'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as',
              'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through',
              'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off',
              'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how',
              'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not',
              'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should',
              'now', 'small']
replacement_patterns = [
    (r'won\'t', 'will not'),
    (r'can\'t', 'cannot'),
    (r'i\'m', 'i am'),
    (r'ain\'t', 'is not'),
    (r'(\w+)\'ll', '\g<1> will'),
    (r'(\w+)n\'t', '\g<1> not'),
    (r'(\w+)\'ve', '\g<1> have'),
    (r'(\w+)\'s', '\g<1> is'),
    (r'(\w+)\'re', '\g<1> are'),
    (r'(\w+)\'d', '\g<1> would')
]
patterns = [(re.compile(regex), repl) for (regex, repl) in replacement_patterns]


def startnendindex(text, sent, both=True):
    tok_ins = OrderedDict()
    special_chars_to_strip = [',', '.', '!', '#']
    for spchar in special_chars_to_strip:
        sent = sent.strip(spchar)
    sent = sent.replace("& amp ;", '&amp;').replace(' amp ', '&amp;')
    #     text = h.unescape(text)
    # sent = sent if isinstance(sent, unicode) else sent.decode('utf8', 'ignore')
    # sent = str(sent)
    #     sent = h.unescape(sent)
    # text = text if isinstance(text, unicode) else text.decode('utf8', 'ignore')
    tokens = re.findall('[0-9a-zA-Z\'\"]+', text.lower())
    words2 = []
    for word in tokens:
        if word.strip().replace('.', '').isdigit():
            words2.append(word)
        else:
            words2 += [x for x in word.split('.') if x.strip() != '']
    tokens = words2
    sent = re.findall('[0-9a-zA-Z\'\"]+', sent.lower())
    words3 = []
    for word in sent:
        if word.strip().replace('.', '').isdigit():
            words3.append(word)
        else:
            words3 += [x for x in word.split('.') if x.strip() != '']
    sent = ''.join(words3).replace("'", "").replace('"', "").replace('#', '').replace('.', '')
    offset = 0
    # print tokens
    for token in tokens:
        offset = text.lower().find(token, offset)
        tok_ins[offset] = token.replace("'", '').replace('"', "").replace('#', '').replace('.', '')
        #         print token.replace("'",'').replace('"',"").replace('#','').replace('.',''), offset
        offset += len(token)

    start_index = -1
    end_index = -1
    start_token_pos = 0
    for x in range(len(tok_ins)):
        try:
            found = ''.join(tok_ins.values()[x:]).index(sent)
            start_index = tok_ins.keys()[x]
            start_token_pos = x
        except ValueError:
            break
    for x in range(0, len(tok_ins) + 1):
        #         print start_token_pos, -x,' '.join(tok_ins.values()[start_token_pos:len(tok_ins)-x]), sent
        try:
            found = ''.join(tok_ins.values()[start_token_pos:len(tok_ins) - x]).index(sent)
            end_index = tok_ins.keys()[len(tok_ins) - x - 1] + len(tok_ins.values()[len(tok_ins) - x - 1])
        except ValueError:
            break
    #     if start_index == -1 or end_index == -1:
    #         print "No Match"
    #     else:
    #         print 'Extracted Text', text[start_index:end_index]
    while (start_index - 1) >= 0 and text[start_index - 1] not in (' ', ',', '!', '?', '\n'):
        start_index -= 1
    while (end_index + 1) < len(text) and text[end_index] not in (' ', ',', '\n'):
        end_index += 1
    while text[start_index] in ('.', '-', ',', '?', '=', '\n'):
        start_index += 1
    while text[end_index - 1] in ('-', ',', '=', '\n'):
        end_index -= 1
    if both == 'start':
        return start_index
    elif both == 'end':
        return end_index
    return start_index, end_index


def snippet_merger(aspect_snippets, output_queue):
    while True:
        try:
            aspect_snippet = aspect_snippets.get(timeout=4)
            if not aspect_snippets.qsize() % 100:
                print aspect_snippets.qsize()
        except Exception as Ex:
            print "Queue is empty. Exiting!", Ex
            output_queue.put('STOP')
            break

        if aspect_snippet:
            for aspect, snippets in aspect_snippet.items():
                for snippet in snippets:
                    sent, review_text = snippet['sentiment_text'], snippet['review_text']
                    try:
                        start_index, end_index = startnendindex(review_text, sent)
                    except UnicodeEncodeError:
                        print "Unicode Encode Error! ", review_text, sent
                        snippet['error'] = True
                        continue

                    snippet['start_index'], snippet['end_index'] = start_index, end_index
                    try:
                        snippet['sentiment_text'] = review_text[start_index:end_index].strip('"').strip("'").strip()
                    except:
                        print 'x', sent, '\n', '\n', review_text

                if len(snippets) <= 1:
                    for snippet in snippets:
                        snippet['overlap'] = 0
                        if 'error' in snippet:
                            del snippet['error']
                    output_queue.put(snippets)
                    continue

                snippets = [x for x in snippets if 'error' not in x]
                len_indexes = len(snippets)
                for x in range(len_indexes):
                    overlapping = False
                    index = snippets[x]
                    start_index1 = index['start_index']
                    end_index1 = index['end_index']
                    review_text = index['review_text']
                    for y in range(x + 1, len_indexes):
                        index2 = snippets[y]
                        start_index2 = index2['start_index']
                        end_index2 = index2['end_index']
                        index_values = [start_index1, end_index1, start_index2, end_index2]
                        if not ((start_index1 > end_index2 and end_index1 > end_index2) or
                                (start_index1 < start_index2 and end_index1 < start_index2)):
                            start_index, end_index = min(index_values), max(index_values)
                            index2['start_index'], index2['end_index'] = start_index, end_index
                            index2['sentiment_text'] = review_text[start_index:end_index]
                            overlapping = True
                            break
                        elif (end_index1 < start_index2):
                            overlapping = connecting_stopword(review_text, end_index1 + 1, start_index2)
                            if overlapping:
                                start_index, end_index = min(index_values), max(index_values)
                                index2['start_index'], index2['end_index'] = start_index, end_index
                                index2['sentiment_text'] = review_text[start_index:end_index]
                                break
                        elif (end_index2 < start_index1):
                            overlapping = connecting_stopword(review_text, end_index2 + 1, start_index1)
                            if overlapping:
                                start_index, end_index = min(index_values), max(index_values)
                                index2['start_index'], index2['end_index'] = start_index, end_index
                                index2['sentiment_text'] = review_text[start_index:end_index]
                                break

                    if not overlapping:
                        index['overlap'] = 0
                    else:
                        index['overlap'] = 1
                        index['related'] = 1
                        index2['related'] = 1
                output_queue.put(snippets)


def connecting_stopword(review_text, start_index, end_index):
    if start_index >= end_index:
        #         print "wrong indexes to check connecting stop words. ", start_index, end_index
        return True
    connecting_text = review_text.lower()[start_index:end_index].strip()
    for (pattern, repl) in patterns:
        connecting_text = re.sub(pattern, repl, connecting_text)
    connecting_words = re.findall('[0-9a-zA-Z]+', connecting_text)
    remaining_words = [word for word in connecting_words if word not in stop_words]
    if not remaining_words:
        return True
    else:
        return False


# Multiprocess child to write output to file
def to_file(output_queue, processes, output_file, columns):
    stop_count = 0
    outputfile = open(output_file, 'w+')
    # ordered_fieldnames = ['partial_review_text', 'product_id', 'confidence_score', 'related',
    #                         'sentiment_text', 'sentiment_type', 'review_text', 'overlap', 'end_index',
    #                         'end_index_partial_review', 'aspect_name', 'start_index_partial_review',
    #                         'file_id', 'category_id', 'start_index', 'review_id', 'product_id2','source']
    print columns
    ordered_fieldnames = columns + ['start_index', 'end_index', 'overlap', 'related']
    writer = csv.DictWriter(outputfile, delimiter=",", quotechar='"', fieldnames=ordered_fieldnames,
                            quoting=csv.QUOTE_ALL)
    writer.writerow(dict((fn, fn) for fn in ordered_fieldnames))
    while True:
        try:
            row = output_queue.get(timeout=4)
        except Queue.Empty:
            time.sleep(60)
            continue
        if row == 'STOP':
            print "Stop count", stop_count
            stop_count += 1
        else:
            for val in row:
                try:
                    writer.writerow(val)
                except UnicodeEncodeError:
                    print val
        if stop_count == processes:
            outputfile.close()
            break


def run_parallel(merged_text_dicts, output_file):
    processes = 4

    manager = mp.Manager()
    review_queue = manager.Queue()

    for review in merged_text_dicts:
        review_queue.put(review)

    output_queue = manager.Queue()
    process_list = [mp.Process(target=snippet_merger, args=(review_queue, output_queue))
                    for x in range(processes)]
    write_process = mp.Process(target=to_file,
                               args=(output_queue, processes, output_file, merged_text_dicts[0].values()[0][0].keys()))
    write_process.start()

    try:
        # Start all the scraping processes
        for process in process_list:
            process.start()

        # Join all the scraping processes after completion
        for process in process_list:
            process.join()
        write_process.join()
        print "Completed!"

    except (KeyboardInterrupt, SystemExit):
        print "Got Interrupt. Killing the processes!"
        for process in process_list:
            process.join()
            process.terminate()
        write_process.join()
        write_process.terminate()
        raise IndexError


def df_unicode(row):
    row = dict(row)
    for key, value in row.items():
        if isinstance(value, (str, unicode)):
            try:
                row[key] = value.strip().decode('utf8') if not isinstance(value, unicode) else unicode(value.strip())
            except UnicodeDecodeError:
                print value
                sys.exit(0)
    return row


def duplicate_merge(file_id, level=1):
    db = get_engine()
    file_name = "downloads/index_merged_{}_{}.csv".format(file_id, level)
    if level == 1:
        snippet_table = 'sentiment_output_temp'
    elif level == 2:
        snippet_table = 'sentiment_ouput'
    reviews_without_text = pd.read_sql("select * from {} where file_id={};".format(snippet_table, file_id), con=db)
    reviews_without_text.rename(columns={'aspect_name': 'aspect_id'}, inplace=True)
    reviews_without_text = reviews_without_text[['id', 'product_id', 'review_id', 'aspect_id', 'sentiment_text']]
    time.sleep(1)
    review_text = pd.read_sql("select review_id, review_text from source_reviews "
                              "where file_id={};".format(file_id), con=db)

    reviews_with_text = reviews_without_text.merge(review_text, on=['review_id'], how='left')
    reviews_with_text = pd.DataFrame(list(reviews_with_text.apply(df_unicode, axis=1)))
    merged_text_dicts = {}
    for row in reviews_with_text.to_dict('records'):
        source_review_id = row['review_id']
        aspect = row['aspect_id']
        if source_review_id not in merged_text_dicts:
            merged_text_dicts[source_review_id] = {}
        if aspect not in merged_text_dicts[source_review_id]:
            merged_text_dicts[source_review_id][aspect] = []
        merged_text_dicts[source_review_id][aspect].append(row)
    merged_text_dicts = merged_text_dicts.values()

    del reviews_with_text

    outfile_path = file_name.split('.')[0] + 'output.csv'

    run_parallel(merged_text_dicts, outfile_path)

    out_reviews = pd.read_csv(outfile_path, quoting=csv.QUOTE_ALL, delimiter=',')
    # No Snippet Found
    index_no_sentiment_text = out_reviews[['start_index', 'end_index']].apply(
        lambda row: True if row['start_index'] == -1 or row['end_index'] == -1 else False, axis=1)
    errors_snippet_not_found = out_reviews[index_no_sentiment_text]
    if level == 1 and not errors_snippet_not_found.empty:
        cant_find_snip_ids = ', '.join(map(str, map(int, list(errors_snippet_not_found.id.unique()))))
        db.execute("update {} set validate_status='N', validate_comment='Not found in review' "
                   "where id in ({});".format(snippet_table, cant_find_snip_ids))
        out_reviews = out_reviews[~index_no_sentiment_text]

    merged_reviews = out_reviews[out_reviews.overlap == 1]

    if level == 1 and not merged_reviews.empty:
        merged_ids = ', '.join(map(str, map(int, list(merged_reviews.id.unique()))))
        db.execute("update {} set validate_status='N', validate_comment='Overlapping' "
                   "where id in ({});".format(snippet_table, merged_ids))
    for row in out_reviews.to_dict('records'):
        rec_id = row['id']
        start_index = row['start_index']
        end_index = row['end_index']
        sentiment_text = row['sentiment_text']

        query = text("update {} set start_index=:sindx, end_index=:eindx, sentiment_text=:stext " \
                     "where id=:rid".format(snippet_table))
        db.execute(query, {'sindx': start_index, 'eindx': end_index, 'stext': sentiment_text, 'rid': rec_id})
    try:
        os.remove(outfile_path)
    except:
        pass
    db.dispose()

    try:
        os.remove(outfile_path)
    except:
        pass
    return True
