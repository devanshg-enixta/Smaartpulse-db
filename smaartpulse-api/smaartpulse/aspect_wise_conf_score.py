import pandas as pd
import statistics
import yaml
import multiprocessing
import numpy as np
import re
import time
adf = pd.DataFrame()
output_aspect_wise_sql_file =""
def aspect_wise_confidence_score(idf):
 ####################################
    def collector(x):
        list_values =[]
        for row in x.to_dict('records'):
            sentiment = row['sentiment_type'].strip()
            list_full = []
            conf_score = row['confidence_score']
            if sentiment == 'positive' or sentiment == 'most-positive' or sentiment == 'neutral':
                sign = 1
                list_values_pos.append(conf_score)
            elif sentiment == 'negative' or sentiment == 'most-negative':
                sign = -1
                list_values_neg.append(conf_score)
            conf_score_sign = row['confidence_score']*sign
            list_values.append(conf_score_sign)
        return list_values
        
    ###########################################

 
    fragments = idf
    aspect_to_score_id_dict = adf
    fragments.aspect = fragments.aspect.astype(int)
    fragments = fragments.merge(aspect_to_score_id_dict,how='left', left_on='aspect', right_on='aspect_id')
    fragments['aspect_name'] = fragments['aspect_name'].apply(lambda x:str(x).strip())
    sql_file = open(output_aspect_wise_sql_file,'a+')

    review_id_group = fragments.groupby(['aspect_name','product_id'])

    d = dict()
    for groups in review_id_group.groups:
            d[groups] = review_id_group.get_group(groups)
    list_full=[] 
    for group, fragments_df in d.iteritems():
        aspect = group[0]
        source_product_id = group[1]
        list_values = []
        list_values_neg = []
        list_values_pos = []
        list_values=collector(fragments_df)
        conf_mean = statistics.mean(list_values)
        min_weight = -1
        max_weight = +1
        min_max_difference = 1 -(-1)
        total_neg = sum(list_values_neg)
        total_pos = sum(list_values_pos)
        score = (conf_mean - min_weight)/min_max_difference*100
        list_col = [str(aspect),str(source_product_id),str(score)]
        list_full.append(list_col)
        scores = pd.DataFrame(list_full, columns=['aspect_name','product_id','sentiment_score'])


    scores.head()

    scores_group = scores.groupby(['product_id'])
    for x,y in scores_group:
        aspects_present = y['aspect_name'].apply(lambda x:x.strip()).unique()
        aspects_list = aspect_to_score_id_dict.aspect_name.str.lower()
        y['sentiment_score'] = y['sentiment_score'].apply(lambda x: float(x))
        total = float(y['sentiment_score'].mean())
        for z in aspects_list:
            if z not in aspects_present:
                row = pd.Series([x,z,0], index=['product_id', 'aspect_name', 'sentiment_score'])
                y = y.append(row,ignore_index = True)
        n = pd.merge(y, aspect_to_score_id_dict, on='aspect_name')  
        aspect_score_dict = dict(zip(y.aspect_name, y.sentiment_score))
        y= "UPDATE product_score SET "
        z=""
        q=0
        for i in (aspect_score_dict):
            if i not in ['others']:
                q =q+1
                z = z+"score"+str(q)+"="+str(aspect_score_dict[i])+" ,"
        b =y+z +" score"+str(q+1)+"=" +str(total) + ", WHERE product_id=" + str(x) + ", AND score_type='Sentiment';"   
        print >> sql_file, b
    sql_file.close()      
def apply_by_multiprocessing(df,func,**kwargs):
    delivery = []
    workers = kwargs.pop('workers')
    global config
    pool = multiprocessing.Pool(processes=workers)
    x = np.array_split(df, workers)
    pool.map(aspect_wise_confidence_score, [(d)
                                    for d in x])
    pool.terminate()
    pool.close()
    pool.join()
    time.sleep (10)
def aspect_wise_confidence_score_mp(idf,output_file,af):
    global adf
    global output_aspect_wise_sql_file
    output_aspect_wise_sql_file = output_file
    adf = pd.concat([adf,af])
    apply_by_multiprocessing(idf,aspect_wise_confidence_score,axis=1, workers=8) 

        