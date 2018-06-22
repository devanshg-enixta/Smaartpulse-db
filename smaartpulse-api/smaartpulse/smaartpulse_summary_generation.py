import pandas as pd
import random
import re
import statistics
import random
from nltk.tokenize import sent_tokenize
import multiprocessing
import numpy as np
import re
import time
def summary_generator(idf):

    treemap_reviews = idf#pd.read_csv(input_file , delimiter = '~')
    treemap_reviews['product_id'].value_counts()
    list(treemap_reviews)

    treemap_reviews['product_id'] = treemap_reviews['product_id'].apply(lambda x: str(x))
    try:
        treemap_reviews_groups = treemap_reviews.groupby(['product_id'])
    except:
        treemap_reviews_groups = treemap_reviews.groupby(['source_product_id'])
        
    final_reviews_list = []

    def pick_unpersonalized_sent(x):
        personalized_terms = ['we', 'I', 'me', 'my','mine','im']
        for z in x:
            for l in personalized_terms:
                if l not in z.lower().split(" "):
                    sent = z
                    break
        return sent
        

    def conf_pos(x):
        row = list(x)
        conf_score = row[15]
        sentiment = row[11]
        if sentiment == 'positive':
            list_confs_p.append(conf_score)
        elif sentiment == 'negative':
            list_confs_n.append(conf_score)
        

    def pos_neg_reviews(x):
        row = list(x)
        sentiment = row[11]
        sent_text = row[17]
        review_text = row[16]
        if sentiment == 'positive'or 'most-positive' or 'neutral':
            list_sent_text_p.append(sent_text)
            list_review_text_p.append(review_text)
        elif sentiment == 'negative' or 'most-negative':
            list_sent_text_n.append(sent_text)
            list_review_text_n.append(review_text)


    for a, b in treemap_reviews_groups:
        key = a
        final_review = []
        final_review_str = ""
        final_review_str = final_review_str +  " " + str(key) + "^~"
        final_review.append(a)
        treemap_aspect_sorted = b.sort_values(['aspect'])
        treemap_aspect_group =  treemap_aspect_sorted.groupby(['aspect'])

        treemap_aspect_group_counts = b['aspect_keyword_new'].count()
        for c,v in treemap_aspect_group:
            aspect = c
            treemap_aspect_group_fine = v.groupby(['aspect_keyword_new'])
            aspect_noun_counts = v['aspect_keyword_new'].value_counts().nlargest(3)
            aspect_noun_counts_dict = aspect_noun_counts.to_dict()
            aspect_noun_counts_list = []
            for x, y in aspect_noun_counts_dict.iteritems():
                aspect_noun_counts_list.append(x)
            aspect_noun_counts_list_indices = []
            treemap_aspect_group_fine_list = treemap_aspect_group_fine.groups.keys()
            for k in aspect_noun_counts_list:
                index = treemap_aspect_group_fine_list.index(k)
                aspect_noun_counts_list_indices.append(index)
            for x,y in list(treemap_aspect_group_fine): 
                if x in aspect_noun_counts_list:
                    aspect_noun = x
                    counts_senti = list(y['sentiment_type'].value_counts())
                    counts_senti_index =y['sentiment_type'].value_counts().index.tolist()
                    counts_senti_sum = sum(counts_senti)
                    counts_senti_p_per = counts_senti[0]/float(counts_senti_sum)
                    if len(counts_senti) >= 2:
                        counts_senti_n_per = counts_senti[1]/float(counts_senti_sum)
                    else:
                        counts_senti_n_per = 0

                    counts_senti_p_per_100 = str(round(counts_senti_p_per * 100,1)) + str("%")
                    counts_senti_n_per_100 = str(round(counts_senti_n_per * 100,1)) + str("%")

                    counts_senti_per_diff = abs(counts_senti_p_per-counts_senti_n_per)
                    if len(counts_senti) == 1:
                        if counts_senti_index[0] == 'positive':
                            counts_senti = [1,0]
                        elif counts_senti_index[0] == 'negative':
                            counts_senti = [0,1]
                    list_confs_p = []
                    list_confs_n = []
                    list_sent_text_p = []
                    list_sent_text_n = []
                    list_review_text_p = []
                    list_review_text_n = []
                    list(y.apply(conf_pos, axis = 1))
                    list(y.apply(pos_neg_reviews, axis = 1))
                    if counts_senti[0] >= counts_senti[1]:
                        conf_score_avg = statistics.mean(list_confs_p)
                        conf_score_min_diff = [abs(x - conf_score_avg) for x in list_confs_p]
                        min_index = conf_score_min_diff.index(min(conf_score_min_diff))
                        review_text = ""
                        review_text_sentence = ""
                        try:
                            sent_text = list_sent_text_p[min_index]
                        except:
                            sent_text = list_sent_text_p[0]
                        for x in list_review_text_p:
                            if sent_text in str(x):
                                review_text = x
                        if review_text == "":
                            review_text = sent_text
                        review_text_tokenize = sent_tokenize(review_text)
                        for y in review_text_tokenize:
                            if sent_text in y:
                                review_text_sentence = y
                        if review_text_sentence == "":
                            review_text_sentence = review_text
                        personalized_terms = ['we', 'i', 'me','im','mine','my','us']
                        for y in personalized_terms:
                            if re.findall(r'[\s.,\b]'+re.escape(y) + '[\s.,\b]', review_text_sentence.lower().replace("'","")) or re.findall(r'^'+re.escape(y) + '[\s.,\b]', review_text_sentence.lower().replace("'","")) or re.findall(r'^'+re.escape(y) + r'$', review_text_sentence.lower().replace("'","")) or re.findall(r'[\s.,\b]'+re.escape(y)+r'$', review_text_sentence.lower().replace("'","")) != [] or len(review_text_sentence) > 240 or len(review_text_sentence) < 20:
                                try:
                                    sent_text = pick_unpersonalized_sent(list_sent_text_p)

                                    for x in list_review_text_p:
                                        if sent_text in x:
                                            
                                        
                                            review_text = x
                                    if review_text == "":
                                        review_text = sent_text
                                    review_text_tokenize = sent_tokenize(review_text)
                                    for y in review_text_tokenize:
                                        if sent_text in y:
                                            review_text_sentence = y
                                    if review_text_sentence == "":
                                        review_text_sentence = review_text
                                except:
                                    pass
                        review_text_sentence = review_text_sentence.capitalize()
                        final_review.append(review_text_sentence)
                        final_review_str = final_review_str + " " + review_text_sentence + ".~"
                    elif counts_senti[1] > counts_senti[0]:
                        conf_score_avg = statistics.mean(list_confs_n)
                        conf_score_min_diff = [abs(x - conf_score_avg) for x in list_confs_n]
                        min_index = conf_score_min_diff.index(min(conf_score_min_diff))
                        review_text = ""
                        review_text_sentence = ""
                        try:
                            sent_text = list_sent_text_n[min_index]
                        except:
                            sent_text = ""
                        for x in list_review_text_n:
                            if sent_text in x:
                                review_text = x
                        if review_text == "":
                            review_text = sent_text
                        review_text_tokenize = sent_tokenize(review_text)
                        for y in review_text_tokenize:
                            if sent_text in y:
                                review_text_sentence = y
                        if review_text_sentence == "":
                            review_text_sentence = review_text
                        personalized_terms = ['we', 'I', 'me', 'my','mine','Im']
                        for y in personalized_terms:
                            if re.findall(r'[\s.,\b]'+re.escape(y) + '[\s.,\b]', review_text_sentence.lower().replace("'","")) or re.findall(r'^'+re.escape(y) + '[\s.,\b]', review_text_sentence.lower().replace("'","")) or re.findall(r'^'+re.escape(y) + r'$', review_text_sentence.lower().replace("'","")) or re.findall(r'[\s.,\b]'+re.escape(y)+r'$', review_text_sentence.lower().replace("'","")) != [] or len(review_text_sentence) > 240 or len(review_text_sentence) < 20:

                                try:
                                    sent_text = pick_unpersonalized_sent(list_sent_text_n)
                                    for x in list_review_text_n:
                                        if sent_text in x:
                                            review_text = x
                                    if review_text == "":
                                        review_text = sent_text
                                    review_text_tokenize = sent_tokenize(review_text)
                                    for y in review_text_tokenize:
                                        if sent_text in y:
                                            review_text_sentence = y
                                    if review_text_sentence == "":
                                        review_text_sentence = review_text
                                except:
                                    pass
                        review_text_sentence = review_text_sentence.capitalize()
                        final_review.append(review_text_sentence)
                        final_review_str = final_review_str + " " + review_text_sentence + "~"
        final_reviews_list.append(final_review_str)

    final_review_summary = []
    for x in final_reviews_list:
        review_summary = ""
        review_list = []
        for y in x.split("~"):
            if y not in review_list:
                review_summary = review_summary + y
                review_list.append(y)
        review_summary = review_summary.replace("..." , ".")
        review_summary = review_summary.replace(".." , ".")
        review_summary = review_summary.replace(" ." , ".")
        final_review_summary.append(review_summary)

    output =[]

    for x in final_review_summary:
        row = [x.strip()]
        output.append(row)

    output = pd.DataFrame(output)
    return output
def apply_by_multiprocessing(df,func,**kwargs):
    delivery = []
    workers = kwargs.pop('workers')
    global config
    pool = multiprocessing.Pool(processes=workers)
    x = np.array_split(df, workers)
 
    result = pool.map(summary_generator, [(d)
                                    for d in x])
    final =  pd.concat(result)
    pool.terminate()
    pool.close()
    pool.join()
    time.sleep (10)
    return final
def summary_generator_mp(idf):
    df = apply_by_multiprocessing(idf,summary_generator,axis=1, workers=8) 
    return df

