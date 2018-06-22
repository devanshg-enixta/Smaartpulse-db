import pandas as pd
import statistics
import multiprocessing
import numpy as np
import re
import time
output_file_name=""
def smaartpulse_counts_neg_pos(idf):
    global output_file_name
    output_file = open(output_file_name,'a+')
    input_treemap_file = idf#pd.read_csv(input_treemap_file,delimiter='~')
    group_pids = input_treemap_file.groupby(['product_id'])
    
    def collector_sents(x):
        row = dict(x)
        #sentiment = row[11].strip()
        sentiment = row['sentiment_type'].strip()
        #print sentiment.

        #conf_score = row[15]
        conf_score = row['confidence_score']
        #print sentiment
        if sentiment == 'positive' or sentiment == 'most-positive' or sentiment == 'neutral':
            sign = 1
            list_values_pos.append(conf_score)
        elif sentiment == 'negative' or sentiment == 'most-negative':
            sign = -1
            list_values_neg.append(conf_score)

    for product_id, df in group_pids:
        #print product_id
        list_values_pos = []
        list_values_neg = []
        list(df.apply(collector_sents, axis = 1))

        total_neg = len(list_values_neg)
       # print total_neg
        total_pos = len(list_values_pos)
        #print total_pos
        try:
            neg_per = float(total_neg)/float(total_neg+total_pos)
        except:
            #if negative is zero count
            neg_per = 0
        try:
            pos_per = total_pos/float(total_neg+total_pos)
        except:
            #if positive is zero count
            pos_per = 0
        neg_per = round(neg_per,2)
        pos_per = round(pos_per,2)

        print >> output_file, "UPDATE product_score SET score6=" + str(pos_per) + ", score7=" + str(neg_per) + " WHERE product_id= " + str(product_id) +  " AND score_type='Sentiment';"
        
    output_file.close()
def apply_by_multiprocessing(df,func,**kwargs):
    delivery = []
    workers = kwargs.pop('workers')
    global config
    pool = multiprocessing.Pool(processes=workers)
    x = np.array_split(df, workers)
 
    pool.map(smaartpulse_counts_neg_pos, [(d)
                                    for d in x])
    pool.terminate()
    pool.close()
    pool.join()
    time.sleep (10)
def smaartpulse_counts_neg_pos_mp(idf,file):
    global output_file_name
    output_file_name =file
    apply_by_multiprocessing(idf,smaartpulse_counts_neg_pos,axis=1, workers=8) 
