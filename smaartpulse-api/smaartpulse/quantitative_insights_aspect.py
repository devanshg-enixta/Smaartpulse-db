import pandas as pd
from pattern.text.en import singularize
import multiprocessing
import numpy as np
import re
import time
def singularizer(x):
    y = singularize(x)
    return y


def quantitative_insights_by_aspect(idf):
    conf_reviews = idf#pd.read_csv(conf_reviews_infile,delimiter='~')
    output = []   
    conf_reviews['aspect_keyword'] = conf_reviews['aspect_keyword'].apply(lambda x:x.replace('NEG_',""))

    conf_reviews['aspect_keyword'] = conf_reviews['aspect_keyword'].apply(lambda x:x.strip())

    conf_reviews['sentiment_type'] = conf_reviews['sentiment_type'].apply(lambda x:x.strip())
    conf_reviews['sentiment_type'] = conf_reviews['sentiment_type'].apply(lambda x:str(x).strip().replace("most-","").replace("neutral","positive").strip())
    
    conf_reviews['aspect_keyword'] = conf_reviews['aspect_keyword'].apply(singularizer)
    def collector(x):
            row = x
            sentiment = row['sentiment_type']
            if sentiment == 'most-positive' or sentiment == 'positive' or sentiment == 'neutral':
                list_positive.append(sentiment)
            elif sentiment == 'most-negative' or sentiment == 'negative':
                list_negative.append(sentiment)

    
    conf_reviews['aspect_keyword'].value_counts()
    conf_aspect_groups = conf_reviews.groupby(['product_id','aspect','aspect_keyword'])
    col = ["product_id","aspect","aspect_keyword","negative","positive","total","negative_per","positive_per"]
    for x,y in conf_aspect_groups:
        source_product_id = str(x).split(",")[0].replace("(","").replace("'","").strip()
        aspect = str(x).split(",")[1].replace("'","").strip()
        aspect_keyword = str(x).split(",")[2].replace(")","").replace("'","").strip()
        aspect_keyword_count = str(y.shape).split(",")[0].replace("(","")
        list_positive = []
        list_negative = []
        list(y.apply(collector, axis = 1))
        positive = len(list_positive)
        negative = len(list_negative)
        total = positive + negative
        if total ==0:
            total =1
        positive_per = positive/float(total)
        negative_per = negative/float(total)
        row =[str(source_product_id),str(aspect) ,str(aspect_keyword) ,str(negative) ,str(positive) , str(total) ,str(negative_per),str(positive_per)]
        output.append(row)
    output = pd.DataFrame(output)
    output.columns =col
    return output
def apply_by_multiprocessing(df,func,**kwargs):
    delivery = []
    workers = kwargs.pop('workers')
    global config
    pool = multiprocessing.Pool(processes=workers)
    x = np.array_split(df, workers)
 
    result = pool.map(quantitative_insights_by_aspect, [(d)
                                    for d in x])
    final =  pd.concat(result)
    pool.terminate()
    pool.close()
    pool.join()
    time.sleep (10)
    return final
def quantitative_insights_by_aspect_mp(idf):
    df = apply_by_multiprocessing(idf,quantitative_insights_by_aspect,axis=1, workers=8) 
    return df

