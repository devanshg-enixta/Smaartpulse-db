# coding: utf-8

import re
import os
import time
import logging
import argparse
from collections import namedtuple
from gensim.utils import smart_open, to_utf8, tokenize
from utils import *
from nltk.tokenize import word_tokenize
import math
import glob
import pandas as pd
import pandas as pd
import csv
import unicodedata
import glob
import argparse
import math, re, sys, fnmatch, string
import pandas as pd
import unicodecsv as csv
import enchant
import os
from gensim.utils import to_unicode, smart_open, dict_from_corpus
import pandas as pd
import logging
import argparse
from collections import namedtuple
from gensim.utils import smart_open, to_utf8, tokenize
from nltk.tokenize import word_tokenize
import math
from nltk.util import ngrams
import random,time
import datetime
from itertools import *
from progressbar import ProgressBar, Percentage, Bar, ETA
import numpy as np
def get_ngrams(text ):
    list1 =[]
    for i in range(1,len(text.split(' '))):
        n_grams = ngrams(word_tokenize(text), i)
        list1.append([ ' '.join(grams) for grams in n_grams])
    list1=list(chain.from_iterable(list1))
    return list1

def fine_grained_sentiment_search(idf,config,phrase_dir,negation_words_file,rerun):
    cnt =0
    output =[]
    aspect = []
    aspect_sent =[]
    aspect_keyword = []
    sent_keyword=[]
    window = 10
    delimiter ='~'
    data_annotation_dir = config.data_annotation_dir

    df = pd.DataFrame(idf)
    if df.empty:
        return df
    df = df[~df.review_text.isnull()]
    temp = pd.DataFrame() 


    


    ####################################### positive keywords #######################################

    pos_file = os.path.join(data_annotation_dir, 'sentiments/positive.txt')
    f = open(pos_file, 'r+')
    keyword = f.readlines()
    keyword =[i.replace('\n','').replace('\t','').replace('  ',' ') for i in keyword]
    positive = [words for segments in keyword for words in segments.split('\r')]

    ####################################### negative keywords #######################################

    neg_file = os.path.join(data_annotation_dir, 'sentiments/negative.txt')
    f = open(neg_file, 'r+')
    keyword = f.readlines()
    keyword =[i.replace('\n','').replace('\t','').replace('  ',' ') for i in keyword]
    negative = [words for segments in keyword for words in segments.split('\r')]

 
    #################################################################################################
    ####################################### negation keywords keywords #######################################

    f = open(negation_words_file, 'r+')
    keyword = f.readlines()
    # neg_words_regex = '|'.join(r'%s' % neg_word for neg_word in keyword)
    # regex = r'\b' + r'(?:%s)' % neg_words_regex + r'\b' + r'[\w\s_]+'

    keyword =[i.replace('\n','').replace('\t','').replace('  ',' ').replace('\ ',' ') for i in keyword]
    negation_words = [words for segments in keyword for words in segments.split('\r')]


    #################################################################################################
    ####################################### Aspect keywords #######################################

    aspects_dir =  os.path.join(data_annotation_dir, 'aspects/')
    aspects = {}
    for aspect_file in glob.glob(aspects_dir + "*.txt"):
        asp_name = aspect_file.replace(aspects_dir,"").split('.txt')[0]
        f = open(aspect_file, 'r+')
        keywords = f.readlines()
        keywords =[i.replace('\n','').replace('\t','').replace('  ',' ') for i in keywords]
        asp_key = [words for segments in keywords for words in segments.split('\r')]
        aspects[asp_name] = asp_key
    ##########################################################################################################
    ####################################### Aspect-Sentiments keywords #######################################

    aspects_sent_dir =  os.path.join(data_annotation_dir, 'aspects-sentiments/')
    aspects_sentiments = {}
    for aspect_sent_file in glob.glob(aspects_sent_dir + "*.txt"):
        asp_name = aspect_sent_file.replace(aspects_sent_dir,"").split('.txt')[0]
        f = open(aspect_sent_file, 'r+')
        keywords = f.readlines()
        keywords =[i.replace('\n','').replace('\t','').replace('  ',' ') for i in keywords]
        asp_key = [words for segments in keywords for words in segments.split('\r')]
        aspects_sentiments[asp_name] = asp_key

    ##########################################################################################################
    ##########################################################################################################

    for row in df.to_dict('records'):
       # try:
       
        sentence = row['sentence'].replace("-", " ").strip().lower().replace('(',"'").replace(')',"'")


        n_sentence =get_ngrams(sentence)

        pos_sent_find = set(positive)&set(n_sentence) 
        pos_sent_match = sorted(pos_sent_find, key = lambda k : n_sentence.index(k))

        neg_sent_find = set(negative)&set(n_sentence) 
        neg_sent_match = sorted(neg_sent_find, key = lambda k : n_sentence.index(k))

        flag =0 
        for asp in aspects:
            keyword = aspects[asp]
            find = set(keyword)&set(n_sentence) 
            match = sorted(find, key = lambda k : n_sentence.index(k))
            if any(match) :
                for asp_keyword  in match:
                    for pos in pos_sent_match:
                        text = re.findall(r''+asp_keyword+'.*?'+pos,sentence)
                        if any(text):
                            flag = 1
                            for txt in text:
                                row['aspect'] = asp
                                row['aspect_keyword'] = asp_keyword
                                row['sentiment_keyword'] = pos
                                row['sentiment_type']= 'positive'
                                n_txt = get_ngrams(txt)
                                negate = set(negation_words)&set(n_txt) 
                                negate_match = sorted(negate, key = lambda k : n_txt.index(k))
                                #print txt,"---->",negate_match
                                if any(negate_match):
                                   row['sentiment_type']= change_sentiment_polarity('positive')
                                else:
                                    negate = set(negation_words)&set(n_sentence) 
                                    negate_match = sorted(negate, key = lambda k : n_sentence.index(k))
                                    if any(negate_match):
                                        row['sentiment_type']= change_sentiment_polarity('positive')
                                        # txt_list = re.findall(r''+negate_match[0]+'.*?'+txt,sentence)
                                        # if any(txt_list):
                                        #     txt = txt_list[0]
                              #  row['sentiment_text'] = txt
                                row['STATUS'] = 2
                                row['WHY'] = "Aspect"
                                output.append(row.copy())
                        if flag == 0:
                            text = re.findall(r''+pos+'.*?'+asp_keyword,sentence)
                            if any(text):
                                for txt in text:
                                    row['aspect'] = asp
                                    row['aspect_keyword'] = asp_keyword
                                    row['sentiment_keyword'] = pos
                                    row['sentiment_type']= 'positive'
                                    n_txt = get_ngrams(txt)
                                    negate = set(negation_words)&set(n_txt) 
                                    negate_match = sorted(negate, key = lambda k : n_txt.index(k))
                                    #print txt,"---->1",negate_match
                                    if any(negate_match):
                                       row['sentiment_type']= change_sentiment_polarity('positive')
                                    else:
                                        negate = set(negation_words)&set(n_sentence) 
                                        negate_match = sorted(negate, key = lambda k : n_sentence.index(k))
                                        if any(negate_match):
                                            row['sentiment_type']= change_sentiment_polarity('positive')
                                            # txt_list = re.findall(r''+negate_match[0]+'.*?'+txt,sentence)
                                            # if any(txt_list):
                                            #     txt = txt_list[0]
                                    #row['sentiment_text'] = txt
                                    row['STATUS'] = 2
                                    row['WHY'] = "Aspect"
                                    output.append(row.copy())

                    flag = 0
                    for neg in neg_sent_match:
                        text = re.findall(r''+asp_keyword+'.*?'+neg,sentence)
                        if text:
                            flag =1
                            for txt in text:
                                row['aspect'] = asp
                                row['aspect_keyword'] = asp_keyword
                                row['sentiment_keyword'] = neg
                                row['sentiment_type']= 'negative'
                                n_txt = get_ngrams(txt)
                                negate = set(negation_words)&set(n_txt) 
                                negate_match = sorted(negate, key = lambda k : n_txt.index(k))
                                #print txt,"---->2",negate_match
                                if any(negate_match):
                                   row['sentiment_type']= change_sentiment_polarity('negative')
                                else:
                                    negate = set(negation_words)&set(n_sentence) 
                                    negate_match = sorted(negate, key = lambda k : n_sentence.index(k))
                                    if any(negate_match):
                                        row['sentiment_type']= change_sentiment_polarity('negative')
                                #         txt_list = re.findall(r''+negate_match[0]+'.*?'+txt,sentence)
                                #         if any(txt_list):
                                #             txt = txt_list[0]
                                # #row['sentiment_text'] = txt
                                row['STATUS'] = 2
                                row['WHY'] = "Aspect"
                                output.append(row.copy())
                        if flag == 0:
                            text = re.findall(r''+neg+'.*?'+asp_keyword,sentence)
                            if any(text):
                                for txt in text:
                                    row['aspect'] = asp
                                    row['aspect_keyword'] = asp_keyword
                                    row['sentiment_keyword'] = neg
                                    row['sentiment_type']= 'negative'
                                    n_txt = get_ngrams(txt)
                                    negate = set(negation_words)&set(n_txt) 
                                    negate_match = sorted(negate, key = lambda k : n_txt.index(k))
                                    #print txt,"---->3",negate_match
                                    if any(negate_match):
                                       row['sentiment_type']= change_sentiment_polarity('negative')
                                    else:
                                        negate = set(negation_words)&set(n_sentence) 
                                        negate_match = sorted(negate, key = lambda k : n_sentence.index(k))
                                        if any(negate_match):
                                            row['sentiment_type']= change_sentiment_polarity('negative')
                                            # txt_list = re.findall(r''+negate_match[0]+'.*?'+txt,sentence)
                                            # if any(txt_list):
                                            #     txt = txt_list[0]

                                 #   row['sentiment_text'] = txt
                                    row['STATUS'] = 2
                                    row['WHY'] = "Aspect"

                                    output.append(row.copy())

        for asp in aspects_sentiments:
            keyword = aspects_sentiments[asp]
            find = set(keyword)&set(n_sentence) 
            match = sorted(find, key = lambda k : n_sentence.index(k))
            if any(match) :
                for asp_keyword  in match:
                    name = asp.split('_')[0]
                    sent = asp.split('_')[-1].split('.txt')[0]
                    row['aspect'] = name
                    row['aspect_keyword'] = asp_keyword
                    row['sentiment_keyword'] = asp_keyword
                    row['sentiment_type'] = sent
                    txt = asp_keyword
                    negate = set(negation_words)&set(n_sentence) 
                    negate_match = sorted(negate, key = lambda k : n_sentence.index(k))
                    #print txt,"---->4",negate_match
                    if any(negate_match):
                        row['sentiment_type']= change_sentiment_polarity('negative')
                        # txt_list = re.findall(r''+negate_match[0]+'.*?'+txt,sentence)
                        # if any(txt_list):
                        #     txt = txt_list[0]
                #    row['sentiment_text'] = txt
                    row['STATUS'] = 2
                    row['WHY'] = "Aspect-sentiment"
                    output.append(row.copy())




    
    output = pd.DataFrame(output)
    
    return output
# ##############################################################################
