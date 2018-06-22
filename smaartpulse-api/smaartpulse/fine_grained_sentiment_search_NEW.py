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
import pandas as pd
import math
from nltk.util import ngrams
from itertools import *
def get_ngrams(text ):
    list1 =[]
    for i in range(1,4):
        n_grams = ngrams(word_tokenize(text), i)
        list1.append([ ' '.join(grams) for grams in n_grams])
    list1=list(chain.from_iterable(list1))
    return list1

def fine_grained_sentiment_search(idf,config,phrase_dir,negation_words_file,rerun):
    cnt =0
    output =[]
    
    delimiter ='~'
    df = pd.DataFrame(idf)
    if df.empty:
        return df
    df = df[~df.review_text.isnull()]
    temp = pd.DataFrame()
   
    phrase_dict, global_dict = dict_of_phrases(phrase_dir=phrase_dir)
    negation_words_list = load_negation_words(negation_words_file=
                                              negation_words_file)


    for line_num,row in enumerate(df.to_dict('records')):
        #try:
        
        sentence = row['sentence'].replace("'","").replace("-", " ").strip().lower()
        window = len(sentence.split())
        toks1 = word_tokenize(sentence.lower())
        #toks1 = get_ngrams(sentence.lower())
        temp_dict = dict()

        if len(toks1) > 1:

            temp_dict = valid_phrase_search(tokens=toks1,
                                            all_phrases_dict=global_dict,
                                            window=window)
        #print temp_dict
       
        # Keeping only super-phrases from the phrases found
        line_phrase_dict = check_super_phrase(phrase_list=temp_dict.keys(),
                                              phrase_dict=temp_dict,
                                              window=window)
        #print line_phrase_dict,"--->"

        # remove the side overlapping phrases.
        toks, line_phrase_list, string = filter_side_overlapping_phrases(line_phrase_dict, tokens=toks1)

        neg_words_regex = '|'.join(r'%s' % neg_word for neg_word in negation_words_list)
        regex = r'\b' + r'(?:%s)' % neg_words_regex + r'\b' + r'[\w\s_]+'

        transformed = re.sub(regex,
                             lambda match: re.sub(r'(\s+)(\w+)', r'\1NEG_\2', match.group(0), count=7),
                             string)

        transformed2 = re.sub(r'[\w\s_]+\b(?:except)\b',
                              lambda match: re.sub(r'(\s*)(\w+)', r'\1NEG_\2', match.group(0), count=5),
                              transformed)

        transformed_tokens = transformed2.split()

        # Search for the corresponding phrase in the aspect and sentiment category
        valid_tuples = list()
        tag_tuple = namedtuple("tag_tuple", ['category', 'class_name', 'keyword', 'pos_index'])
        flag =0
        x = ['sentiments','aspects-sentiments']
        for i in x:
            for cat_type in phrase_dict[i]:
                find = set(line_phrase_list)&set(phrase_dict[i][cat_type]) 
                match = sorted(find, key = lambda k : line_phrase_list.index(k))
                if any(match):
                    print match


        # line_phrase_list contains the tags identified for the review
        for tag in line_phrase_list:
            for category in phrase_dict:
                tag_underscore = tag.replace(" ", "_")
                tag_new = tag_underscore
                # if "sentiments" == category or "aspects-sentiments" == category:
                #     if 'NEG_' + tag_underscore in transformed_tokens:
                #         tag_new = 'NEG_' + tag_underscore
                
                for cat_type in phrase_dict[category]:
                    #print cat_type,"------>",category
                    if tag in phrase_dict[category][cat_type]:
                       # print category,cat_type
                       # print category,cat_type
                        if category == "sentiments":
                            if 'NEG_' + tag_underscore in transformed_tokens:
                                if flag ==0:
                                    tag_new = 'NEG_' + tag_underscore
                                    flag =1
                           # print tag_new,"---->",string
                        if category == "aspects-sentiments":
                            aspect_type = cat_type.split("_")[0]
                            sent_type = cat_type.split("_")[1]
                            if tag_new == 'NEG_' + tag_underscore:
                                sent_type = change_sentiment_polarity(sent_type)

                            aspect_tuple = (category.split("-")[0], aspect_type, tag_new)
                            #print aspect_tuple
                            sent_tuple = (category.split("-")[1], sent_type, tag_new)
                            row['aspect'] = aspect_tuple[1]
                            row['aspect_keyword'] = aspect_tuple[2]
                            row['sentiment_type'] = sent_tuple[1]
                            row['sentiment_keyword'] = sent_tuple[2]
                            row['STATUS'] = 2
                            row['WHY'] = "Aspect-sentiment"
                            output.append(row.copy())  

                        else:

                            tag = tag.replace(" ", "_")

                            try:
                                tag_index = toks.index(tag)
                            except ValueError:
                                pass
                                
                            if tag_new == 'NEG_' + tag_underscore:
                                cat_type = change_sentiment_polarity(cat_type)
                            valid_tuples.append(tag_tuple(category, cat_type, str(tag_new), tag_index))

        sorted_aspect_sent_list = sorted(valid_tuples, key=lambda x: x.pos_index)

        for k, aspect_tuple in enumerate(sorted_aspect_sent_list):
            if aspect_tuple.category == 'aspects':
                
                sent_tuple = min(sorted_aspect_sent_list,
                                 key=lambda x: abs(x.pos_index - aspect_tuple.pos_index) if (
                                     x.category == 'sentiments') else 1000)

                if sent_tuple.category == "sentiments":

                    if math.fabs(aspect_tuple[-1] - sent_tuple[-1]) < 15:
                        row['aspect'] = aspect_tuple[1]
                        row['aspect_keyword'] = aspect_tuple[2]
                        row['sentiment_type'] = sent_tuple[1]
                        row['sentiment_keyword'] = sent_tuple[2]
                        row['STATUS'] = 2
                        row['WHY'] = "Aspect"
                        output.append(row.copy())
   
    output = pd.DataFrame(output)
   
    return output
# ##############################################################################
