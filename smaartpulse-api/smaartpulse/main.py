# -*- coding: utf-8 -*-
import os
import argparse
from datetime import datetime
import remove_bad_reviews as rbr
import review_cleaning as rc
import sentence_tokenize_nltk as stn
import pandas as pd
import numpy as np
import re
import regex
import difflib
#import fine_grained_sentiment_search as fgss
import fine_grained_sentiment_search_NEW as fgss
import fine_grained_emotion_search_NEW as fges
import conf_scores_indexes_sentiment as csis
import treemap_domain_polarity_change_sentiment as tdpcs
import treemap_domain_polarity_change_emotion as tdpce
import conf_scores_indexes_emotion as csie
from snippet_correction import correct_snippets
import datetime
import traceback
from sqlalchemy import create_engine
import os
import smaartpulse_summary_generation as ssg
import star_rating_cal as strc
import quantitative_insights_aspect as quias
import smaartpulse_counts as sc
import aspect_wise_conf_score as awcs
import psutil
from itertools import *
import time
from progressbar import ProgressBar, Percentage, Bar, ETA
import multiprocessing
from multiprocessing import Pool as ThreadPool
import os.path
import logging
from prepare_metadata import LexiconMetadata
from time import gmtime, strftime
from datetime import datetime
import csv


i=0
file_name_pattern = ""
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
config =None
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
def index_finding(df):
    output= []
    for row in df.to_dict('records'):
        sentence = row['sentence'].lower()
        review = row['review_text']
        sentiment_text = row['sentiment_text']
        sts = review.lower().strip().find(sentiment_text.lower().strip())
        lts = len(sentiment_text.strip())+sts
        sts,lts = check(review[sts:lts].lower().strip(),sentiment_text.lower().strip(),sts,lts)
        
        row['start_index'] = sts
        row['end_index'] = lts
        if sentiment_text.strip().lower() != review[sts:lts].strip().lower():
            row['flag'] = 0
            temp_rev = re.sub("\s\s+" , " ", review)
            temp_new_sent = re.sub("\s\s+" , " ", review[sts:lts])
            temp_sent_text = re.sub("\s\s+" , " ", sentiment_text)
            sts = temp_rev.lower().strip().find(temp_sent_text.lower().strip())
            lts = len(sentiment_text.strip())+sts
            if sts == -1:
                print "pass"
                pass
            else:
                while temp_sent_text.lower().strip() != temp_new_sent.lower().strip():
                    sts,lts = check(review[sts:lts].lower().strip(),sentiment_text.lower().strip(),sts,lts)
                    temp_new_sent = re.sub("\s\s+" , " ", review[sts:lts])
                    if lts > len(review):
                        row['flag'] =0
                        break
                row['start_index'] = sts
                row['end_index'] = lts
                row['flag'] =1
        else:
            row['flag'] =1

        output.append(row.copy())
    output = pd.DataFrame(output)
    return output

def run_smartpulse(df,rerun):
    output =[]
    error=pd.DataFrame()
    global config
    global file_name_pattern
    global i
    category_id = config.category_id
    input_reviews = config.input_file
    input_df = df
    classification = config.classification_type
    client_id = config.client
    file_id = config.file_id
    output_folder = config.output_dir
    logging_folder = config.log_dir
    category = config.category
    final_file = config.final_file
    file_name_pattern = str(category_id) + '_' + str(file_id)
    domain_words_dir = config.domain_words_dir
    reviews_data_dir = config.input_dir
################################################################################################################################################################
################################################################################################################################################################
################################################################################################################################################################

    stus = 0
    idf = pd.DataFrame()
    if rerun ==0:
        idf = pd.read_sql_query("SELECT * from final_output_{}".format(file_id), config.local_db)
        if idf.empty:
            pass
        else:
            stus = idf.STATUS.max()
            diff_temp = idf[~idf.review_id.isin(input_df.review_id.values)]
            if diff_temp.shape[0] > 10:
                pass
            else:
                df = idf[idf.STATUS == stus]
################################################################################################################################################################
################################################################################################################################################################
################################################################################################################################################################

    if idf.empty or rerun ==1 and not df.empty:
        df = apply_by_multiprocessing_1(df,sent_tokenise,rerun,axis=1, workers=8)
        df.to_sql('final_output_{}'.format(config.file_id), config.local_db, if_exists='append',index=False)
    phrase_dir = config.data_annotation_dir
    print "done 1"
    negations_words_file=os.path.join(BASE_DIR, "data_annotation","negation_words.txt")
    window = 15
################################################################################################################################################################
################################################################################################################################################################
################################################################################################################################################################
    if stus ==1 or rerun ==1 and not df.empty:
        df = apply_by_multiprocessing_2(df,fine_grained_sentiment,phrase_dir,negations_words_file,rerun,axis=1, workers=8) 

        df.to_sql('final_output_{}'.format(config.file_id), config.local_db, if_exists='append',index=False)
    print "done 2"
################################################################################################################################################################
################################################################################################################################################################
################################################################################################################################################################
    if stus ==2 or rerun ==1 and not df.empty:
        df = apply_by_multiprocessing_3(df,conf_score_func,negations_words_file,rerun,axis=1, workers=8) 
        df.to_sql('final_output_{}'.format(config.file_id), config.local_db, if_exists='append',index=False)
    print "done 3"
 ###############################################################################################################################################################
################################################################################################################################################################
################################################################################################################################################################
    if stus ==3 or rerun ==1 and not df.empty:
        df= apply_by_multiprocessing_4(df,treemap_func,domain_words_dir,rerun,axis=1, workers=8) 
        #df = index_finding(df)
        print "done indexes"
        df.to_sql('final_output_{}'.format(config.file_id), config.local_db, if_exists='append',index=False)
    print "done 4"
    

################################################################################################################################################################
################################################################################################################################################################
################################################################################################################################################################
    if category_id == 35:
################################################################################################################################################################
################################################################################################################################################################
############################################# Star Rating#################################################################################################
######################################### Qnatitative Insights#############################################################################
##########################################Summary Generation##################################################################################################
#################################Buysmart Aspect WIse confidence score####################################################################################################
################################################################################################################################################################
################################################################################################################################################################
################################################################################################################################################################
        if rerun ==1:
            idf =df
        if config.local_db.dialect.has_table(config.local_db, 'star_rating_{}'.format(config.file_id)):
            pass
        else:
            try:
                star_df = strc.star_rating_mp(idf)
            except:
                idf = pd.read_sql_query("SELECT * from final_output_{}".format(file_id), config.local_db)
                star_df = strc.star_rating_mp(idf)  

            star_df.to_sql('star_rating_{}'.format(config.file_id), config.local_db, if_exists='append',index=False)
        print "done star rating"
    ################################################################################################################################################################
    ################################################################################################################################################################
    ################################################################################################################################################################
        qdf = idf[idf.STATUS == 3]
        if config.local_db.dialect.has_table(config.local_db, 'Qunatitative_insights_{}'.format(config.file_id)):
            pass
        else:
            quant_df = quias.quantitative_insights_by_aspect_mp(qdf)
            quant_df.to_sql('Qunatitative_insights_{}'.format(config.file_id), config.local_db, if_exists='append',index=False)
        print "done quantitative insights"
    ################################################################################################################################################################
    ################################################################################################################################################################
    ################################################################################################################################################################
        try: 
            col_sum = ["source",'review_id','product_id','product_name','review_date','star_rating','reviewer_id','reviewer_name','review_url','review_tag','review_text','product_id']
            sum_df = pd.read_csv(os.path.join(config.output_dir,"Summary_temp_file_"+str(config.category)+".csv"),encoding='utf-8',names=col_sum)  
            sumary_df = ssg.summary_generator(sum_df)
            config.local_db.execute("Drop table if exists summary_temp_table_{};".format(file_id))
            sumary_df.to_sql('Summarized_output_{}'.format(config.file_id), config.local_db, if_exists='append',index=False)
            print "done Summarized_output"
        except:
            print "error in Summarized_output"
            pass
    ################################################################################################################################################################
    ################################################################################################################################################################
    ################################################################################################################################################################
        asp_df = idf[idf.STATUS == 4]
        output_aspect_wise_sql_file = os.path.join(output_folder,"aspect_wise_prod_id_conf_score_"+str(category_id)+".sql")
        output_smaartpulse_counts_file = os.path.join(output_folder,"smaartpulse_counts_by_prod_id_"+str(category_id)+".sql")
        if os.path.exists(output_aspect_wise_sql_file):
            pass
        else:
            asp_id_df = pd.read_sql_query("select * from category_aspect where category_id ={};".format(category_id), config.con)
            asp_id_df = asp_id_df[['aspect_name','id']]
            asp_id_df=asp_id_df.rename(index=str, columns={"id": "aspect_id"})
            awcs.aspect_wise_confidence_score_mp(asp_df,output_aspect_wise_sql_file,asp_id_df)
        print "done aspect_wise_conf_score"
        if os.path.exists(output_smaartpulse_counts_file):
            pass
        else:
            sc.smaartpulse_counts_neg_pos_mp(asp_df,output_smaartpulse_counts_file)
        print "done smaartpulse_counts_neg_pos"

################################################################################################################################################################
################################################################################################################################################################
################################################################################################################################################################


    return True



def sent_tokenise(args):
    df,rerun= args
    status = stn.sentence_tokenizer(df,config,i)
    return status
def fine_grained_sentiment(args):
    df,phrase_dir,negations_words_file,rerun= args
    status = fgss.fine_grained_sentiment_search(df,config,phrase_dir,negations_words_file,rerun)
    return status
def conf_score_func(args):
    df,negations_words_file,rerun= args
    status = csis.confidence_scores_indexes_sentiment(df,config,negations_words_file,rerun)
    return status
def treemap_func(args):
    df,domain_words_dir,rerun= args
    status= tdpcs.treemap_domain_polarity_changer_sentiment(df,config,domain_words_dir,rerun)
    if config.category_id ==35:
        summ.to_csv(os.path.join(config.output_dir,"Summary_temp_file_"+str(config.category)+".csv"),mode='a',header=False,index=False,encoding="utf-8")
    return status

def get_engine(user_details):
    engine = create_engine(
        'mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}?charset=utf8mb4'.format(**user_details),
        encoding='utf-8')
    return engine

def apply_by_multiprocessing_1(df,func,rerun,**kwargs):
    delivery = []
    workers = kwargs.pop('workers')
    global config
    pool = multiprocessing.Pool(processes=workers)
    x = np.array_split(df, workers)
 
    result = pool.map(sent_tokenise, [(d,rerun)
                                    for d in x])
    final =  pd.concat(result)
    pool.terminate()
    pool.close()
    pool.join()
    time.sleep (10)
    return final

def apply_by_multiprocessing_2(df,func,phrase_dir,negations_words_file,rerun ,**kwargs):
    delivery = []
    workers = kwargs.pop('workers')
    global config
    pool = multiprocessing.Pool(processes=workers)
    x = np.array_split(df, workers)
 
    result = pool.map(fine_grained_sentiment, [(d,phrase_dir,negations_words_file,rerun)
                                    for d in x])
    final =  pd.concat(result)
    pool.terminate()
    pool.close()
    pool.join()
    time.sleep (10)
    return final
def apply_by_multiprocessing_3(df,func,negations_words_file,rerun ,**kwargs):
    delivery = []
    workers = kwargs.pop('workers')
    global config
    pool = multiprocessing.Pool(processes=workers)
    x = np.array_split(df, workers)
 
    result = pool.map(conf_score_func, [(d,negations_words_file,rerun)
                                    for d in x])
    final =  pd.concat(result)
    pool.terminate()
    pool.close()
    pool.join()
    time.sleep (10)
    return final
def apply_by_multiprocessing_4(df,func,domain_words_dir,rerun ,**kwargs):
    delivery = []
    workers = kwargs.pop('workers')
    global pool
    pool = multiprocessing.Pool(processes=workers)
    x = np.array_split(df, workers)
 
    result= pool.map(treemap_func, [(d,domain_words_dir,rerun)
                                    for d in x])
    final =  pd.concat(result)
    pool.terminate()
    pool.close()
    pool.join()
    time.sleep (10)

    return final
def trigger_smaartpulse(config1):
    x=time.time()
    client = ""
    global config
    config =config1
    client_id = config.client
    category_id = config.category_id
    file_id = config.file_id
    local_db = config.local_db
    lexicon_db = config.con
    root_dir = config.root_dir
    input_reviews = config.input_file
    classification = config.classification_type
    sdb = 1
    start = time.time()
    now = datetime.now()
    date = str(now.year) + "-" + str(now.month) + "-" + str(now.day)
    final_dir  = root_dir
    # # Todo: Create a db connection for lexicon db and local db for interim data
    conn = local_db.connect(close_with_result=True)
    conn1 = lexicon_db.connect(close_with_result=True)
    input_df = pd.read_sql_query("SELECT * from reviews_temp_{}".format(file_id), local_db)
    review_snippets_name = 'final_output_{}'.format(file_id)

    try:
        if sdb ==1:
            local_db.execute("Drop table if exists {}".format(review_snippets_name))
            local_db.execute("Drop table if exists summary_temp_table_{};".format(file_id))
            local_db.execute("Drop table if exists star_rating_{}".format(config.file_id))
            local_db.execute("Drop table if exists Qunatitative_insights_{}".format(config.file_id))
            sdb =1
        else:
            sdb =0
    except:
        pass

    if sdb ==1:
        try:
            os.remove(os.path.join(config.output_dir,"Summary_temp_file_"+str(config.category)+".csv"))
        except:
            pass
        review_snippets_table_query = "CREATE TABLE {} (`STATUS` bigint(20) DEFAULT 0, " \
                                  "`category_id` bigint(20) NULL, `file_id` bigint(20) NULL, " \
                                  "`flag` bigint(20) NULL, " \
                                  "`product_id` varchar(255) NULL DEFAULT '', " \
                                  "`review_id` varchar(255) NULL DEFAULT '', " \
                                  "`source` varchar(255) NULL DEFAULT '', " \
                                  "`review_text` text DEFAULT NULL, " \
                                  "`review_title` varchar(1024) NULL DEFAULT '', " \
                                  "`sentence` text DEFAULT NULL, " \
                                  "`star_rating` varchar(255) COLLATE utf8_bin DEFAULT NULL," \
                                  "`aspect` varchar(1024) NULL DEFAULT '', " \
                                  "`aspect_keyword` varchar(1024) NULL DEFAULT '', " \
                                  "`sentiment_type` varchar(1024) NULL DEFAULT '', " \
                                  "`sentiment_keyword` varchar(1024) NULL DEFAULT '', " \
                                  "`confidence_score` double(5,2) DEFAULT NULL, " \
                                  "`sentiment_text` text DEFAULT NULL, " \
                                  "`WHY` varchar(255) NULL DEFAULT '', " \
                                  "`review_text_tag_ind` varchar(999) DEFAULT NULL, " \
                                  "`treemap_name` varchar(999) DEFAULT NULL, " \
                                  "`start_index` int(11) DEFAULT NULL, " \
                                  "`end_index` int(11) DEFAULT NULL, " \
                                  "`start_index_partial_review` int(11) DEFAULT NULL, " \
                                  "`end_index_partial_review` int(11) DEFAULT NULL," \
                                  "`partial_review_text` text DEFAULT NULL)" \
                                  "ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;".format(review_snippets_name)
        local_db.execute(review_snippets_table_query)

#    config = LexiconMetadata(root_dir=root_dir, file_id=file_id, client_id=client_id, category_id=category_id, db=lexicon_db, local_db=local_db, input_file=input_reviews,final_dir=final_dir, classification_type='sentiment')               

    input_df['review_text'] = input_df['review_text'].apply(lambda x: x.encode('ascii','ignore').decode('ascii'))
    input_df['review_text'] = input_df['review_text'].astype(str)
    length = input_df.shape[0]
    print "DB loading done..."
    try:
        del input_df['id']
    except:
        pass
    for i in range(0, length/10000+1):
        start = time.time()
        df = input_df[i*20000:(i+1)*20000]
        if len(df) <1:
            break
        run_smartpulse(df,sdb)
        end = time.time()
        print "Chunk {} done in :- {}".format(i+1,end-start)
    cols_treemap = ["source",'review_id','product_id','aspect','sentiment_text','review_text_tag_ind', 'start_index_partial_review','end_index_partial_review',
                'start_index','end_index','confidence_score','treemap_name','category_id',
                'sentiment_type','partial_review_text','review_text','flag']
    treemap_outfile = os.path.join(config.output_dir, "Treemap_outputfile_"+file_name_pattern+".txt")          
    final_df = pd.read_sql_query("SELECT * from final_output_{} where STATUS =4".format(file_id), local_db)
    final_df = final_df[cols_treemap]
    final_df.to_csv(treemap_outfile,sep = '~', index = False,encoding='utf-8')
    diff_df = input_df[~input_df.review_id.isin(final_df.review_id.values)]
    diff_df['STATUS'] = 5
    diff_df.to_sql('final_output_{}'.format(config.file_id), config.local_db, if_exists='append',index=False)

#    diff_df.to_csv(os.path.join(config.output_dir, 'Reviews_not_processed_'+str(config.category_id)+'.csv'),quoting = csv.QUOTE_ALL,index=False,encoding = 'utf-8')
    y=time.time()
    print "Smaartpulse Generation Completed Succcessfully in:- {}".format(y-x)
 
 
if __name__ == "__main__":
    global rerun
    parser = argparse.ArgumentParser("Smaartpulse")

    parser.add_argument('--input_reviews_file',
                        type=str,
                        help='')
    parser.add_argument('--category_id',
                        type=int)
    parser.add_argument('--classification_type',
                        type=str)
    parser.add_argument('--client_id',
                        type = int)
    parser.add_argument('--file_id',
                        type = int)
    parser.add_argument('--rerun',
                        type = str)
    parser.add_argument('--check',
                    type = str)

    rerun =0
    global config

    args = parser.parse_args()
    root_dir = os.path.join(os.getcwd(), 'lexicons_db')
    input_reviews = os.path.join(root_dir,args.input_reviews_file)
    final_dir  = root_dir
    local_db = get_engine({'user':'root','passwd':'','host':'localhost','port':3306,'db':'smaartpulse'})
    lexicon_db = get_engine({'user':'root','passwd':'','host':'localhost','port':3306,'db':'saasmmst'})
    conn = local_db.connect(close_with_result=True)
    conn1 = lexicon_db.connect(close_with_result=True)
 
    config = LexiconMetadata(root_dir=root_dir, file_id=args.file_id, client_id=args.client_id, category_id=args.category_id, db=lexicon_db, local_db=local_db, input_file=input_reviews,final_dir=final_dir, classification_type='sentiment')
    trigger_smaartpulse(config)

 