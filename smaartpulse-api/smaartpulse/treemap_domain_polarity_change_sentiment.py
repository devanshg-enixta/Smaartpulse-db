# -*- coding: utf-8 -*-
from nltk.corpus import stopwords
import pandas as pd
import nltk
import random
import os
import re
import argparse
from time import gmtime, strftime
from datetime import datetime
import pandas as pd
import csv
import numpy as np
from snippet_correction import correct_snippets
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV
from collections import OrderedDict
import re
from gensim.utils import smart_open, to_utf8, tokenize
import cPickle
from collections import Counter


def dict_of_phrases(phrase_dir):
    phrase_dict = dict()
    global_dict = dict()


    for DIR in os.listdir(phrase_dir):
        phrase_dict[DIR] = dict()
        if DIR.startswith("."):
            continue
        for infile in os.listdir(os.path.join(phrase_dir, DIR)):
            if infile.startswith("."):
                continue
            if infile.endswith(".txt") :
                fname = infile.split(".txt")[0]
                phrase_dict[DIR][fname] = dict()

                # reading the phrases in file
                with open(phrase_dir + "/" + DIR + "/" + infile, 'rb') as foo:
                    #if "/r" in foo:
                        #foo = foo.split("/r")


                    line_num = 0
                    for line in foo:
                        if "\r" in line:
                            linee = line.split("\r")
                            for line in linee:
                                line_num += 1
                                try:
                                    line = line.encode('ascii','ignore').decode('ascii').replace("_", " ").replace("\n\r", "\n")
                                except:
                                    line = line.replace("_", " ").replace("\n\r", "\n")
                                line = line.replace("-", " ")
                                phrase = line.strip().split("|")[0]

                                # check for empty line
                                if phrase == "":
                                    continue

                                if phrase not in phrase_dict:
                                    phrase_dict[DIR][fname][phrase] = 1
                                    global_dict[phrase] \
                                        = 1
                        else:
                            line_num += 1
                            line = line.decode('ascii','ignore').replace("_", " ").replace("\n\r","\n")

                            # Replacing dashes with spaces so as to prevent errors
                            # in detecting word boundaries
                            line = line.replace("-", " ")
                            phrase = line.strip().split("|")[0]
                            if phrase == "":
                                continue

                            if phrase not in phrase_dict:
                                phrase_dict[DIR][fname][phrase] = 1
                                global_dict[phrase] = 1
                                    
    return phrase_dict, global_dict


def treemap_domain_polarity_changer_sentiment(idf,config,domain_words_dir,rerun):
    def changing_domain_pol(x):
        #row = dict(x)
        row =x
        
        sent_keyword = row['sentiment_keyword'].strip()
        sent_keyword_No_NEG = row['sentiment_keyword'].replace("NEG_","").strip()

        aspect_keyword = row['aspect_keyword'].strip()
        sentiment = row['sentiment_type'].strip()
        aspect = row['aspect']
        if sent_keyword_No_NEG in positives:
            sentiment = 'positive'
            if "NEG_" in sent_keyword:
                sentiment = 'negative'
            else:
                sentiment = 'positive'
        elif sent_keyword_No_NEG in negatives:
            sentiment = 'negative'
            if "NEG_" in sent_keyword:
                sentiment = 'positive'
            else:
                sentiment = 'negative'
        row['sentiment_type'] = sentiment
        return row
    temp = pd.DataFrame()
    reviews = pd.DataFrame(idf)
    if reviews.empty:
        return reviews

    reviews = reviews[~reviews.review_text.isnull()]
    reviews['review_text'] = reviews['review_text'].apply(lambda x: unicode(x).encode("utf-8"))
 
    phrase_dir = domain_words_dir
    phrase_dict, global_dict = dict_of_phrases(phrase_dir)

    reviews_group = reviews.groupby(["aspect"])
    d = {}
    for groups in reviews_group.groups:
        path = str(groups)
        d[path] = reviews_group.get_group(groups)
        
    dict_aspect_groups = {}
    df_empty = pd.DataFrame(columns=['source', 'review_id', 'product_id', 'product_name', 'review_date',
                                     'star_rating', 'verified_user', 'reviewer_name', 'review_url', 'sentence',
                                     'aspect', 'sentiment_type', 'aspect_keyword', 'sentiment_keyword', 'predictions',
                                     'confidence_score', 'review_text', 'sentiment_text', 'new_indexes', 'start_index',
                                     'end_index','WHY','flag'])
    printcounter = 0
    for aspect, aspect_group in d.iteritems():
        printcounter = printcounter + 1
        if (printcounter == 1000):
            printcounter = 0

        positives_dict = phrase_dict['sentiments'][aspect+"_domain_positive"]
        positives = []
        for x,y in positives_dict.iteritems():
            positives.append(x)
            
        negatives_dict = phrase_dict['sentiments'][aspect+"_domain_negative"]
        negatives = []
        for x,y in negatives_dict.iteritems():
            negatives.append(x)
        dict_aspect_groups['aspect_'+aspect] = aspect_group.apply(changing_domain_pol, axis = 1)

        df_empty = pd.concat([df_empty,dict_aspect_groups['aspect_'+aspect]])

    df_empty_3 = df_empty
    df_empty_3['treemap_title'] = ""
    df_empty_3['treemap_name'] = df_empty_3['aspect_keyword']
    df_empty_3['source'] = 1
    df_empty_3['review_text_tag_ind'] = ""
    df_empty_3['category_id'] = config.category_id
    df_empty_3['review_tag'] = ""
    df_empty_3['reviewer_id'] = ""
#    df_empty_3.to_csv(summary_output_file, sep = '~', index = False,encoding='utf-8')

    cols_treemap = ["source",'review_id','product_id','aspect','sentiment_text', 'review_text_tag_ind',"sentence",
                    'start_index','end_index','confidence_score','treemap_name','category_id',
                    'sentiment_type','review_text','WHY','flag']
    cols_source_review = ["source",'review_id','product_id','product_name','review_date','star_rating','reviewer_id','reviewer_name','review_url','review_tag','review_text','product_id']
      
    treemap_table = df_empty_3[cols_treemap]
    d = {'most-positive': 'positive', 'most-negative': 'negative','positive':'positive','negative':'negative','neutral':'neutral'}

    treemap_table['sentiment_type'] = treemap_table['sentiment_type'].map(d)
    treemap_table = treemap_table.drop(treemap_table[treemap_table.sentiment_type == 'neutral'].index)
    treemap_table['aspect'] = treemap_table['aspect'].apply(lambda x:str(x).strip())
    treemap_table['aspect'] = treemap_table['aspect'].apply(lambda x:str(x).replace("vfm","value for money"))
    treemap_table['STATUS'] = 4
    treemap_table['start_index_partial_review'] = 0
    treemap_table['end_index_partial_review'] = 0
    treemap_table['partial_review_text'] = ""
    treemap_table['sentiment_type'] = treemap_table['sentiment_type'].apply(lambda x:str(x).strip())
    treemap_table['category_id'] = config.category_id
    source_reviews_table = df_empty_3[cols_source_review]
    source_reviews_table = source_reviews_table.drop_duplicates(subset='review_id',inplace = True)
   # try:
    #final_df = correct_snippets(treemap_table, config)
    # except:

    return treemap_table
