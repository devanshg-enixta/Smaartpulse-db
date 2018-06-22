__author__ = "Mohammed Murtuza"
__copyright__ = "Copyright 2017, Enixta Innovations"

from nltk.corpus import stopwords
import pandas as pd
import nltk
#import matplotlib.pyplot as plt
import random
import os
import re
import argparse
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
from gensim.utils import smart_open, to_utf8, tokenize
import cPickle
from collections import Counter


def dict_of_phrases(phrase_dir):
    phrase_dict = dict()
    global_dict = dict()


    # Making a dict() for all the phrases we have
    # and ensure that the phrases don't have underscores in them

    for DIR in os.listdir(phrase_dir):
        #print DIR
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
                            #print "true there is \r is here"
                            linee = line.split("\r")
                            #line = .join(line)
                            for line in linee:
                                #print line
                                line_num += 1

                                # Replacing underscores with spaces
                                line = line.decode('ascii', 'ignore').replace("_", " ").replace("\n\r", "\n")

                                # Replacing dashes with spaces so as to prevent errors
                                # in detecting word boundaries
                                line = line.replace("-", " ")
                                phrase = line.strip().split("|")[0]
                                #print phrase

                                # check for empty line
                                if phrase == "":
                                    #print line_num
                                    continue

                                if phrase not in phrase_dict:
                                    phrase_dict[DIR][fname][phrase] = 1
                                    global_dict[phrase] \
                                        = 1
                        else:
                            #print line
                            line_num += 1

                            # Replacing underscores with spaces
                            line = line.decode('ascii','ignore').replace("_", " ").replace("\n\r","\n")

                            # Replacing dashes with spaces so as to prevent errors
                            # in detecting word boundaries
                            line = line.replace("-", " ")
                            phrase = line.strip().split("|")[0]
                            #print phrase

                            # check for empty line
                            if phrase == "":
                                #print line_num
                                continue

                            if phrase not in phrase_dict:
                                phrase_dict[DIR][fname][phrase] = 1
                                #phrase_dict[sentiments][positive][good] = 1
                                #{sentiments:{'positive':{'good': 1, 'awesome': 1}}}
                                #{'good': 1, 'awesome': 1, 'bad':1}

                                global_dict[phrase] = 1
                                    
    return phrase_dict, global_dict



#########################




##################


    
##################



def treemap_domain_polarity_changer_emotion(reviews_highlighted_file,domain_words_dir,source_reviews_outfile,treemap_outfile,summary_outfile,category):

    def aspect_keyword_check(x):
        row = list(x)
        sent_keyword = row[13].strip()
        #print sent_keyword
        aspect_keyword = row[12].strip()
        #print aspect_keyword
        aspect_keyword_new = aspect_keyword
        if aspect_keyword == sent_keyword:
            #print "yes"
            aspect_keyword_new = ""
        elif "NEG_" + aspect_keyword == sent_keyword:
            aspect_keyword_new = ""
        return aspect_keyword_new


    def sent_words_collector(x):
        row = list(x)
        #print row
        #sentiment_keyword = row[13].replace('NEG_',"")
        sentiment_keyword = row[13]
        #print sentiment_keyword

        sentiment = row[11].strip()
        #print sentiment
        if sentiment == 'joy':
            sent_list_joy.append(sentiment_keyword)
            
        elif sentiment == 'sadness':
            sent_list_sadness.append(sentiment_keyword)

        elif sentiment == 'fear':
            sent_list_fear.append(sentiment_keyword)

        elif sentiment == 'anticipation':

            sent_list_anticipation.append(sentiment_keyword)
        elif sentiment == 'surprise':

            sent_list_surprise.append(sentiment_keyword)
        elif sentiment == 'anger':

            sent_list_anger.append(sentiment_keyword)
        elif sentiment == 'disgust':

            sent_list_disgust.append(sentiment_keyword)
        elif sentiment == 'trust':
            sent_list_trust.append(sentiment_keyword)



    def sentiment_assigner(m):
        #print sent_list_most_neg
        row_3 = list(m)
        #print row_3
        #print row_3
        tree_title = str(row_3[11]).strip()
        #print tree_title
        tree_title_name = ""
        if tree_title == 'joy':
            #print 'positive'
            titles_counter = Counter(sent_list_joy)
            most_common_title = titles_counter.most_common(1)
            #print most_common_title
            #or this?
            most_common_title_str = most_common_title[0][0]
            tree_title_name = most_common_title_str
            row_3.append(tree_title_name)
        elif tree_title == 'sadness':
            #print 'most_positive'
            titles_counter = Counter(sent_list_sadness)
            most_common_title = titles_counter.most_common(1)
            #print most_common_title
            #or maybe this
            most_common_title_str = most_common_title[0][0]
            tree_title_name = most_common_title_str
            row_3.append(tree_title_name)
        elif tree_title == 'disgust':
            #print 'negative'
            titles_counter = Counter(sent_list_disgust)
            most_common_title = titles_counter.most_common(1)
            #print most_common_title
            #or here
            most_common_title_str = most_common_title[0][0]
            tree_title_name = most_common_title_str
            row_3.append(tree_title_name)
        elif tree_title == 'fear':
            #print 'most_negative'
            titles_counter = Counter(sent_list_fear)
            most_common_title = titles_counter.most_common(1)
            #print most_common_title
            #its here
            most_common_title_str = most_common_title[0][0]
            tree_title_name = most_common_title_str
            row_3.append(tree_title_name)
        elif tree_title == 'anger':
            #print 'most_negative'
            titles_counter = Counter(sent_list_anger)
            most_common_title = titles_counter.most_common(1)
            #print most_common_title
            #its here
            most_common_title_str = most_common_title[0][0]
            tree_title_name = most_common_title_str
            row_3.append(tree_title_name)
        elif tree_title == 'anticipation':
            #print 'most_negative'
            titles_counter = Counter(sent_list_anticipation)
            most_common_title = titles_counter.most_common(1)
            #print most_common_title
            #its here
            most_common_title_str = most_common_title[0][0]
            tree_title_name = most_common_title_str
            row_3.append(tree_title_name)
        elif tree_title == 'trust':
            #print 'most_negative'
            titles_counter = Counter(sent_list_trust)
            most_common_title = titles_counter.most_common(1)
            #print most_common_title
            #its here
            most_common_title_str = most_common_title[0][0]
            tree_title_name = most_common_title_str
            row_3.append(tree_title_name)
        elif tree_title == 'surprise':
            #print 'most_negative'
            titles_counter = Counter(sent_list_surprise)
            most_common_title = titles_counter.most_common(1)
            #print most_common_title
            #its here
            most_common_title_str = most_common_title[0][0]
            tree_title_name = most_common_title_str
            row_3.append(tree_title_name)
        else:
            #print 'Other sentiment'
            tree_title_name = tree_title
            row_3.append(tree_title_name)

        return row_3

    #reviews
    #phrase_dir_of_domain_words

    input_reviews = reviews_highlighted_file
    phrase_dir_input = domain_words_dir
    source_reviews_output_file = source_reviews_outfile
    treemap_output_file = treemap_outfile
    summary_output_file = summary_outfile
    category_id = category_id
    
    startTime = datetime.now()
    reviews = pd.read_csv(reviews_highlighted_file, delimiter = '~')
    #reviews = reviews[np.isfinite(reviews['source_product_id'])]

    #reviews.head()

    phrase_dir = domain_words_dir


    phrase_dict, global_dict = dict_of_phrases(phrase_dir)



    aspects_k_group = reviews.groupby(["aspect_keyword",'source_product_id'])
    d2 = {}
    df_empty_3 = pd.DataFrame(columns=['source',     'source_review_id',     'source_product_id',    'product_name',     'review_date',  'star_rating',  'verified_user',    'reviewer_name',    'review_url',   'sentence',     'aspect',   'sentiment',    'aspect_keyword',   'sentiment_keyword',    'predictions',  'conf_score',   'review_text',  'sentiment_text',   'new_indexes',  'start_index',  'end_index', 'treemap_title'])
    for groups in aspects_k_group.groups:
        path = str(groups)
        d2[path] = aspects_k_group.get_group(groups)

    dict_aspect_k_groups = {}   
    for aspect_k, aspect_k_groups in d2.iteritems():
        #print aspect_k
        sent_list_joy = []
        sent_list_sadness = []
        sent_list_anticipation = []
        sent_list_fear =[]
        sent_list_anger = []
        sent_list_trust = []
        sent_list_surprise = []
        sent_list_disgust =[]
        aspect_k_groups.apply(lambda x:sent_words_collector(x), axis = 1)
        #dict_aspect_k_groups['aspect_'+aspect_k.strip()] = aspect_k_groups.apply(sentiment_assigner, axis = 1)
        dict_aspect_k_groups['aspect_'+aspect_k.strip()] = pd.DataFrame(list(aspect_k_groups.apply(sentiment_assigner, axis = 1)),columns=['source',     'source_review_id',     'source_product_id',    'product_name',     'review_date',  'star_rating',  'verified_user',    'reviewer_name',    'review_url',   'sentence',     'aspect',   'sentiment',    'aspect_keyword',   'sentiment_keyword',    'predictions',  'conf_score',   'review_text',  'sentiment_text',   'new_indexes',  'start_index',  'end_index','treemap_title'])

        df_empty_3 = pd.concat([df_empty_3,dict_aspect_k_groups['aspect_'+aspect_k.strip()]])
            #dict_aspect['aspect_'+str(key)] = pd.DataFrame(list(value.apply(featureExtraction, axis=1)), columns=['source','source_review_id','common-id','source-url','aspect','sentiment','sentiment-text','review-tag or review-text','text-start-index','text-end-index','confidence-score','source-mobile-id','aspect_noun']) 

    #need to add one column here!!!!

    #     print aspect_group.apply(changing_domain_pol, axis = 1).head()
            #dict_treemap_sent['treemap_sent'+str(m)] = 
            #pd.DataFrame(list(w.apply(sentiment_labeller, axis = 1)), 
     #columns = ['source','source_review_id','common-id','source-url','aspect','sentiment',
                
                #'sentiment-text','review-tag or review-text','text-start-index','text-end-index','confidence-score','source-mobile-id','aspect_noun','treemap_header', 'treemap_sent'])
    df_empty_3['aspect_keyword_new'] = df_empty_3.apply(lambda x: aspect_keyword_check(x), axis = 1)
    df_empty_3['treemap_name'] = df_empty_3['treemap_title'] + " " +  df_empty_3['aspect_keyword_new']
    df_empty_3['treemap_name'] = df_empty_3['treemap_name'].apply(lambda x: x.replace("NEG_","not ").replace("_", " ").strip())
    df_empty_3['source'] = 31
    df_empty_3['review_text_tag_ind'] = ""
    df_empty_3['category_id'] = category_id
    df_empty_3['product_id'] = ""
    df_empty_3['review_tag'] = ""
    df_empty_3['reviewer_id'] = ""
    df_empty_3.rename(columns = {'conf_score':'confidence-score','sentiment':'sentiment_type'}, inplace = True)

    df_empty_3.to_csv(summary_output_file,sep='~',index=False)
#source~source_review_id~product_id~aspect_id~sentiment_text~review_text_tag_ind~start_index~end_index~confidence-score~source_product_id~treemap_name~category_id~sentiment_type
#source~source_review_id~mobile_id~mobile_name~review_date~star_rating~reviewer_id~reviewer_name~review_url~review_tag~review_text~product_id
    cols_treemap = ["source",'source_review_id','product_id','aspect','sentiment_text', 'review_text_tag_ind', 'start_index','end_index','confidence-score','source_product_id','treemap_name','category_id','sentiment_type']
    cols_source_review = ["source",'source_review_id','source_product_id','product_name','review_date','star_rating','reviewer_id','reviewer_name','review_url','review_tag','review_text','product_id']
    treemap_table = df_empty_3[cols_treemap]
    source_reviews_table = df_empty_3[cols_source_review]

    #source~source_review_id~product_id~aspect_id~sentiment_text~review_text_tag_ind~start_index~end_index~confidence-score~source_product_id~treemap_name~category_id~sentiment_type
    #source~source_review_id~mobile_id~mobile_name~review_date~star_rating~reviewer_id~reviewer_name~review_url~review_tag~review_text~product_id
    source_reviews_table.drop_duplicates(subset='source_review_id',inplace = True)
    source_reviews_table.shape


    source_reviews_table.to_csv(source_reviews_output_file, sep = '~', index = False)
    treemap_table.to_csv(treemap_output_file, sep = '~', index = False)
    #need to add one column here!!!!

    #     print aspect_group.apply(changing_domain_pol, axis = 1).head()
        #dict_treemap_sent['treemap_sent'+str(m)] = 
        #pd.DataFrame(list(w.apply(sentiment_labeller, axis = 1)), 
    #columns = ['source','source_review_id','common-id','source-url','aspect','sentiment',
            
            #'sentiment-text','review-tag or review-text','text-start-index','text-end-index','confidence-score','source-mobile-id','aspect_noun','treemap_header', 'treemap_sent'])


    ##################

    print datetime.now() - startTime




