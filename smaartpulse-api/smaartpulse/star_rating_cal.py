import pandas as pd
import multiprocessing
import numpy as np
import re
import time
def star_rating_calc(idf):
    read = idf
    temp = pd.DataFrame()
    read = read[~read.star_rating.isnull()]
    read = read[~read.STATUS.isnull()]
    if read.empty:
        pass
    else:
        out=[]
        try: 
            read = read.drop(read[read.star_rating == 'na'].index)
        except:
            pass

        read.dropna(subset=['star_rating'],inplace=True)
        product_id = read.product_id.unique()
        col=['product_id','source','1star_count','2star_count','3star_count','4star_count','5star_count','total_count','average_5star','average_pct']
        col1=['product_id','1star_count','2star_count','3star_count','4star_count','5star_count','total_count','average_5star','average_pct']
        read['star_rating'] = read['star_rating'].apply(lambda x:int(str(x).replace("out of 5","").replace("stars","").replace("star","").replace(".0","").replace("null","2").strip()[0]) )
        for ids in product_id:
            total = 0
            avg_5=0.0
            avg_pct=0.0 
            rev1 = read[read.product_id==ids]
            try:
                product_source= rev1.source.unique()
                for source in product_source:
                    total = 0
                    avg_5=0.0
                    avg_pct=0.0 
                    rev = rev1[rev1.source ==source]
                    star_5 = rev[rev.star_rating == 5]
                    star_4 = rev[rev.star_rating == 4]
                    star_3 = rev[rev.star_rating == 3]
                    star_2 = rev[rev.star_rating == 2]
                    star_1 = rev[rev.star_rating == 1]
                    total = len(star_1.index)+len(star_2.index)+len(star_3.index)+len(star_4.index)+len(star_5.index)
                    if total > 0:
                        avg_5= round(float((1*len(star_1.index)+2*len(star_2.index)+3*len(star_3.index)+4*len(star_4.index)+5*len(star_5.index))/float(total)),3)
                        avg_pct = round(float((avg_5-1)*25),3)
                        row = [ids,source,len(star_1.index),len(star_2.index),len(star_3.index),len(star_4.index),len(star_5.index),total,avg_5,avg_pct]
                        out.append(row)
            except:
                total = 0
                avg_5=0.0
                avg_pct=0.0 
                rev = rev1
                star_5 = rev[rev.star_rating == 5]
                star_4 = rev[rev.star_rating == 4]
                star_3 = rev[rev.star_rating == 3]
                star_2 = rev[rev.star_rating == 2]
                star_1 = rev[rev.star_rating == 1]
                total = len(star_1.index)+len(star_2.index)+len(star_3.index)+len(star_4.index)+len(star_5.index)
                if total > 0:
                    avg_5= round(float((1*len(star_1.index)+2*len(star_2.index)+3*len(star_3.index)+4*len(star_4.index)+5*len(star_5.index))/float(total)),3)
                    avg_pct = round(float((avg_5-1)*25),3)
                    row = [ids,len(star_1.index),len(star_2.index),len(star_3.index),len(star_4.index),len(star_5.index),total,avg_5,avg_pct]
                    out.append(row)
        out=pd.DataFrame(out)
        try:
            out.columns = col
        except:
            out.columns = col1
        return out
        
def apply_by_multiprocessing(df,func,**kwargs):
    delivery = []
    workers = kwargs.pop('workers')
    global config
    pool = multiprocessing.Pool(processes=workers)
    x = np.array_split(df, workers)
 
    result = pool.map(star_rating_calc, [(d)
                                    for d in x])
    final =  pd.concat(result)
    pool.terminate()
    pool.close()
    pool.join()
    time.sleep (10)
    return final
def star_rating_mp(idf):
    df = apply_by_multiprocessing(idf,star_rating_calc,axis=1, workers=8) 
    return df


        
    

