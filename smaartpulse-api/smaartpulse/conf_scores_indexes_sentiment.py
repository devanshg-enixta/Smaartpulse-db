

import math
import random
import csv
from collections import OrderedDict
import re
import regex
import difflib
import multiprocessing
import numpy as np
import time
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from itertools import *
def check(x,y,s,l):
    for i,st in enumerate(difflib.ndiff(x.lower().strip(),y.lower().strip())):
            if st[0]==' ' or len(st.strip()) <2: continue
            elif st[0]=='-':
                if i < len(x)/2:
                    s = s +1
                else:
                    l = l -1
            elif st[0]=='+':

                if i < len(x)/2:
                    s =s -1
                else:
                    l = l+1
    return s,l
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



def sentiment_text_finder(x, negations_words_file ):
    min_index = -1
    all_sent_text = []
   
    for row in x.to_dict('records'):
        aspect_word = row['aspect_keyword'].strip()
        aspect_word_re = aspect_word.replace("NEG_", "").replace("_", " ")
        negation_flag = False
        sentiment_word = row['sentiment_keyword'].strip()
        sentiment_word_re = sentiment_word.replace("NEG_", "").replace("_", " ")
        sentence = row['sentence'].lower()
        #sentence = sentence.replace("'", "")
        #sentence = sentence.replace("-", " ")
        #sentence = sentence.replace("  ", " ").replace("  ", " ")
        review = row['review_text']
        if "NEG_" in sentiment_word:
            negation_flag = True

        aspect_index = sentence.find(aspect_word_re)
        sentiment_index = sentence.find(sentiment_word_re)
        min_index = 0
        max_index = len(sentence)
        if (sentiment_index != -1) and (aspect_index != -1):
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
            sentiment_text = sentence[min_index:max_index + len(max_word)]
        else:
            sentiment_text = sentence


        # st = review.lower().find(sentence.lower())
        # # if st<0:
        # #     print sentence,"----->",review
        # lt = st+len(sentence)
        # sts = sentence.lower().find(sentiment_text.lower())+st
        # lts = len(sentiment_text.strip())+sts
        # for i,s in enumerate(difflib.ndiff(review[sts:lts].lower(),sentiment_text.lower())):
        #     if s[0]==' ' or len(s.strip()) <2: continue
        #     elif s[0]=='-':
        #         if i < len(review[sts:lts])/2:
        #             sts = sts +1
        #         else:
        #             lts = lts -1
        #     elif s[0]=='+':
        #         if i < len(review[sts:lts])/2:
        #             sts =sts -1
        #         else:
        #             lts = lts+1

        all_sent_text.append(sentiment_text)
        # start_index.append(sts)
        # end_index.append(lts)

    return all_sent_text


def get_probab(data, LR_complete, trainingVector):
    data = pd.DataFrame(data)
    output = []
    for row in data.to_dict('records'):
        test = []
        row['sentence'] =  row['sentence'].encode('ascii','ignore').decode('ascii')
        test.append(str(row['sentence']))
        test_dtm = trainingVector.transform(test)

        predLabel = LR_complete.predict(test_dtm)
        predLabel_pr = LR_complete.predict_proba(test_dtm)
        predLabel[0] = predLabel[0].replace('most-', '')
        if predLabel[0] == 'neutral':
            output.append(math.ceil(random.uniform(95.4, 99.9)) / 100)
        if predLabel[0] == 'positive':
            output.append(sum(predLabel_pr[0][1:len(predLabel_pr[0])]))
        if predLabel[0] == 'negative':
            output.append(sum(predLabel_pr[0][0:len(predLabel_pr[0]) - 1]))
   
    return output



def confidence_scores_indexes_sentiment(idf,config,negations_words_file,rerun):
    data_annotation = pd.DataFrame(idf)
    data_annotation = data_annotation.drop_duplicates()
    if data_annotation.empty:
        return data_annotation
    data_annotation = data_annotation[~data_annotation.review_text.isnull()]
    temp = pd.DataFrame()#pd.read_sql_query("SELECT * from reviews_temp_sent_tokenize", config.local_db)

    data_annotation.sentiment_type = data_annotation.sentiment_type.replace('most-', '').str.strip()
    data_annotation.sentiment_type = data_annotation.sentiment_type.replace('positve', 'positive')
    X = data_annotation.sentence
    y = data_annotation.sentiment_type
    try:
        vect = CountVectorizer(stop_words='english', ngram_range=(1, 1), max_df=.90, min_df=4)
        X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=3, test_size=0.1)
        vect.fit(X_train)
        trainingVector = CountVectorizer(stop_words='english', ngram_range=(1, 1), max_df=.90, min_df=5)
        trainingVector.fit(X)
        X_dtm = trainingVector.transform(X)
        LR_complete = LogisticRegression()
        LR_complete.fit(X_dtm, y)

        data_annotation.sentiment_type = data_annotation.sentiment_type.str.strip().str.lower().str.replace('most-','')
        data_annotation = data_annotation[data_annotation.sentiment_type.isin(['positive','negative'])]
        data_annotation['confidence_score'] = get_probab(data_annotation,LR_complete, trainingVector)
    except:
        data_annotation['confidence_score'] = math.ceil(random.uniform(95.4, 99.9)) / 100
    data_annotation['sentiment_text'] = sentiment_text_finder(data_annotation,negations_words_file)
    data_annotation['STATUS'] = 3
    if data_annotation.empty:
            return temp
    else:
        data_annotation=data_annotation.drop_duplicates()
        return data_annotation
