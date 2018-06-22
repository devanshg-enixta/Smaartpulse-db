
from nltk.tokenize import sent_tokenize
from gensim.utils import to_unicode, smart_open, to_utf8
import argparse
import logging
import re
import pandas as pd
import csv
import glob
import nltk
import itertools
from itertools import *
import os,re,sys
import pickle
import shutil

def sentence_tokenizer(idf,config,rerun):
    output =[]
    df = pd.DataFrame(idf)
    if df.empty:
        return df
    df = df[~df.review_text.isnull()]

    df['review_text'] = df['review_text'].apply(lambda x: x.encode('ascii','ignore'))
  #  df['review_text'] = df['review_text'].apply(lambda x:str(x).replace('\n', " ").replace('\t', " ").replace('\r', " ").replace("~"," ").strip())
    df['review_text'] = df['review_text'].apply(lambda x: str(x).replace('###', '.'))
    regex = r"[\r]+"
    replace_regex = r"."
    df['review_text'] = df['review_text'].apply(lambda x: re.sub(regex, replace_regex, str(x)))
    regex = r"[\n]+"
    replace_regex = r"."
    df['review_text'] = df['review_text'].apply(lambda x: re.sub(regex, replace_regex, str(x)))
    regex = r"[\t]+"
    replace_regex = r"."
    df['review_text'] = df['review_text'].apply(lambda x: re.sub(regex, replace_regex, str(x)))
    temp = pd.DataFrame()

    try:
        del df['product_name']
    except:
        pass

    df['sentence'] = ""

    # Open reviews file and split the reviews by sentences
    for row in df.to_dict('records'):

        row['review_text']=row['review_text'].encode('ascii','ignore').decode('ascii')
        nltk_sent_output = sent_tokenize(str(row['review_text']).strip())



        # splitting the sentence on comma
        for sentence in nltk_sent_output:
            flag = 0
            x = nltk.word_tokenize(sentence)
            mynewlist = [s for s in x if '.' in s]
            if mynewlist:
                t = mynewlist[0].split('.')
                t = [s for s in t if s.isdigit()]
                if not t:
                    sentence = sentence.replace('.'," ; ")
            sentence_1 = sentence.strip()
            sp = re.compile(re.escape(' but '))
            sentence_1 = re.sub(sp," ; ",sentence_1)
            sp = re.compile(re.escape(' however '))
            sentence_1 = re.sub(sp," ; ",sentence_1)
            sp = re.compile(re.escape(' except '))
            sentence_1 = re.sub(sp," ; ",sentence_1)
            sentence_1 = re.sub(r'\bbut\b',";",sentence_1)
            sentence_1 = re.sub(r'\bhowever\b',";",sentence_1)
            test = re.sub(r'\band\b',"~",sentence_1)
            x = nltk.word_tokenize(sentence_1)
            mynewlist = [s for s in x if ',' in s]
            if mynewlist:
                t = mynewlist[0].split(',')
                t = [s for s in t if s.isdigit()]
                if not t:
                    sentence = sentence.replace(','," ; ")
            new_sent_list_1 = re.split('[;\n\r\t?!]',sentence_1)
            new_test = re.split('[;\n\r\t?!]',test)

            for z in new_sent_list_1:
                if ',' in z:
                    z = z.split(',')
                    for test_sent in z:
                        if len(test_sent.split(' ')) <2:
                            flag =1

            if flag ==0:
                new_sent_list_1 = re.split('[,;\n\r\t?!]',sentence_1)
            else:
                pass
            flag =0
            for z in new_test:
                if '~' in z:
                    z = z.split('~')
                    for test_sent in z:
                        if len(test_sent.split(' ')) <3:
                            flag =1
            if flag ==0:
                sentence_1 = re.sub(r'\band\b',"~",sentence_1)
                new_sent_list_1 = re.split('[,;\n\r\t?!~]',sentence_1)
            else:
                pass



            new_sent_list = []
            new_sent_list.append(new_sent_list_1)
            new_sent_list=list(chain.from_iterable(new_sent_list))
            new_sent_list = [words for segments in new_sent_list for words in segments.replace('\n','').split('\r')]
            new_sent_list  = filter(None, new_sent_list )

            for new_sent in new_sent_list:
                if len(new_sent.split()) > 1:
                    new_sent = new_sent.strip()
                    row['sentence'] = new_sent
                    #row['start_index_sentence'] = row['review_text'].find(new_sent)
                    row['STATUS'] = 1
                    output.append(row.copy())
        # except:
        #     pass
    output = pd.DataFrame(output)
    output['sentiment_text'] = ""
    if output.empty:
        return temp
    else:
        output=output.drop_duplicates()
        return output
            
