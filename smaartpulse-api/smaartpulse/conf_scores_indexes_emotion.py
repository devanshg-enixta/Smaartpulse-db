__author__ = "Mohammed Murtuza"
__copyright__ = "Copyright 2017, Enixta Innovations"

from nltk.corpus import stopwords
import pandas as pd
import nltk
#import matplotlib.pyplot as plt
import random
import os
import re
from time import gmtime, strftime
from datetime import datetime
import pandas as pd
import csv
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV
from collections import OrderedDict
import re
#from gensim.utils import smart_open, to_utf8, tokenize
import cPickle
import argparse
from collections import Counter

def conf_score_assigner(x):
    row = list(x)
    sentiment = row[11].strip()
    #print sentiment
    predictions = row[14]
    if sentiment == 'positive' or sentiment == 'most-positive' or sentiment == 'neutral':
        conf_score = predictions[1]
    elif sentiment == 'negative' or sentiment == 'most-negative':
        conf_score = predictions[0]
    return conf_score

def negation_word_inclusion(negations_words_file, sentence, word_index):

    # reading the entire file using readlines()
    neg_words_list = open(negations_words_file, mode='rb').readlines()

    # striping the "/n" at the end of every line
    neg_words_list = list(word.strip() for word in neg_words_list)

    for neg_word in neg_words_list:
        if neg_word in sentence:
            # finding the index of the negative word
            neg_word_index = sentence.find(neg_word)

            # if negative word occurs before word, then return the negative word index
            if neg_word_index < word_index:
                sentence_fragment = sentence[neg_word_index:word_index]

                # check if number of words between neg word and word is < 6
                if len(sentence_fragment.split()) < 15:
                    return neg_word_index

    return word_index


def startnendindex2(text, sent):
    tok_ins = OrderedDict()
    tokens= re.findall('[0-9a-zA-Z\'\"]+', text.lower())
    words2 = []
    for word in tokens:
        if word.strip().replace('.','').isdigit():
            words2.append(word)
        else:
            words2 += [x for x in word.split('.') if x.strip()!='']
    tokens = words2
    sent = re.findall('[0-9a-zA-Z\'\"]+', str(sent).lower())
    words3 = []
    for word in sent:
        if word.strip().replace('.','').isdigit():
            words3.append(word)
        else:
            words3 += [x for x in word.split('.') if x.strip()!='']
    sent = ''.join(words3).replace('"',"").replace('#','').replace('.','')
    offset = 0
    # print tokens
    for token in tokens:
        offset = text.lower().find(token, offset)
        tok_ins[offset] = token.replace("'",'').replace('"',"").replace('#','').replace('.','')
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
    #     print tok_ins
    for x in range(0,len(tok_ins)+1):
    #         print start_token_pos, -x,' '.join(tok_ins.values()[start_token_pos:len(tok_ins)-x]), sent
        try:
            found = ''.join(tok_ins.values()[start_token_pos:len(tok_ins)-x]).index(sent)
            end_index = tok_ins.keys()[len(tok_ins)-x-1] + len(tok_ins.values()[len(tok_ins)-x-1])
        except ValueError:
            break
    return start_index, end_index

def sentiment_text_finder(x,negations_words_file):
    row = list(x)
    aspect_word = row[12].strip()
    #print aspect_word
    aspect_word_re = aspect_word.replace("NEG_","").replace("_"," ")
    #print aspect_word_re
    negation_flag = False
    sentiment_word = row[13].strip()
    sentiment_word_re = sentiment_word.replace("NEG_","").replace("_"," ")
    #print sentiment_word
    sentence = row[9]
    #sentence_preprocessing
    sentence = sentence.replace("'", "")

    # Replacing dashes with spaces so as to prevent errors
    # in detecting word boundaries
    sentence = sentence.replace("-", " ")
    #replacing double spaces with single spaces and tabs with spaces
    sentence = sentence.replace("  "," ").replace("  "," ")
    #print sentence
    review = row[16]
    if "NEG_" in sentiment_word:
        negation_flag = True
    
    aspect_index = sentence.find(aspect_word_re)
    #print aspect_index
    sentiment_index = sentence.find(sentiment_word_re)
    #print sentiment_index
    if (sentiment_index != -1) and (aspect_index != -1):
        # finding the minimum and maximum index among aspect and sentiment words
        if aspect_index >= sentiment_index:
            min_index = sentiment_index
            max_index = aspect_index
            max_word = aspect_word_re
        else:
            min_index = aspect_index
            max_index = sentiment_index
            max_word = sentiment_word_re
    if negation_flag:
        min_index = negation_word_inclusion(negations_words_file, sentence, min_index)
    sentiment_text = sentence[min_index:max_index+len(max_word)]
    #print sentiment_text
    return sentiment_text

def higher_func_index(x):
    row = list(x)
    #print row
    review = row[16].lower().replace("'"," ")
    #print review
    sent_text = row[17]
    #print sent_text
    start_index, end_index = startnendindex2(review, sent_text)
    tuple_indexes = (start_index, end_index)
    if start_index == -1 or end_index == -1:
        print "indexes are -1"
    #print start_index, end_index
    return tuple_indexes


def confidence_scores_indexes_emotion(category,input_annotated_file,raw_reviews,negations_words_file,output_file,classifier):
    
    startTime = datetime.now()
    category = category
    input_annotated_file = input_annotated_file
    raw_reviews_input = raw_reviews
    negations_words_file = negations_words_file
    output_file = output_file
    classifier_file = classifier
    data_annotation = pd.read_csv(input_annotated_file, delimiter = '~',quoting=csv.QUOTE_NONE)

    raw_reviews = pd.read_csv(raw_reviews_input, delimiter = '~')
    print classifier_file
    classifier = cPickle.load(open(classifier_file,'r'))
    data_annotation['sentence'].head()

    #data_annotation['predictions'] = list(np.round_(classifier.predict_proba(data_annotation['sentence'].apply(lambda x: str(x))),decimals = 7))
    data_annotation['predictions'] = 1
    #data_annotation['conf_score'] =data_annotation.apply(conf_score_assigner, axis = 1)
    data_annotation['conf_score'] = 1
    data_annotation_merge = pd.merge(data_annotation,raw_reviews[['source_review_id','review_text']], on = 'source_review_id')

    data_annotation_merge['sentiment_text'] = data_annotation_merge.apply(lambda x: sentiment_text_finder(x,negations_words_file), axis = 1)
    data_annotation_merge.shape

    data_annotation_merge['new_indexes'] = data_annotation_merge.apply(lambda x: higher_func_index(x), axis = 1)
    #data_annotation_merge.to_csv("/Users/apple/Documents/intermediate_output.txt", index=False, sep = '~')
    s = data_annotation_merge['new_indexes'].apply(lambda x: str(x).split(','))
    data_annotation_merge['start_index'] = s.apply(lambda x: x[0])
    data_annotation_merge['start_index'] = data_annotation_merge['start_index'].apply(lambda x: x.replace("(",""))
    data_annotation_merge['end_index'] = s.apply(lambda x: x[1])
    data_annotation_merge['end_index'] = data_annotation_merge['end_index'].apply(lambda x: x.replace(")",""))
    #data_annotation_merge['indexess'] =data_annotation_merge[['review_text','sentiment_text']].head(10).apply(lambda (x,y): startnendindex2(x,y), axis = 1)
    data_annotation_merge.to_csv(output_file,index = False, sep = '~')



    print datetime.now() - startTime




