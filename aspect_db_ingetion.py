import glob
import itertools
from itertools import *
import os,re,sys
import pickle
import shutil
import pandas as pd
import time
import sys
path = os.getcwd()
category=sys.argv[1]
aspects = path+"/fine_grained_phrases_"+category+"_flipkart_sentiment/aspects"
asp_sent = path+"/fine_grained_phrases_"+category+"_flipkart_sentiment/aspects-sentiments"
sent_path = path+"/fine_grained_phrases_"+category+"_flipkart_sentiment/sentiments"
domain = "sentiments"
category_id =sys.argv[2]

i = 1
aspect_df=[]
for file in glob.glob(aspects+'/*.txt'):
    aspect_name = file.split('/')[-1].split('.txt')[0]
    row = [i,category_id,aspect_name,'',time.strftime('%Y-%m-%d %H:%M:%S'),time.strftime('%Y-%m-%d %H:%M:%S'),'A']
    i=i+1
    aspect_df.append(row)
aspect_df = pd.DataFrame(aspect_df)
aspect_df.columns = ['id','category_id','aspect_name','aspect_desc','created_date','modified_date','status']
aspect_df.to_csv('category_aspect_{}_{}.csv'.format(category_id,category),index=False)

asp_key = []
temp_df = pd.DataFrame()
for file in glob.glob(aspects+'/*.txt'):
    name = file.split('/')[-1].split('.txt')[0]
    temp_df.aspect = aspect_df.aspect_name.apply(lambda x: x.lower().strip())
    temp_df_id = aspect_df.id[temp_df.aspect == name]
    f = open(file,'rb')
    lines =f.readlines()
    lines = [words for segments in lines for words in segments.replace('\n','').split('\r')]
    lines = filter(None, lines)
    for line in lines:
        row = ['',1,category_id,temp_df_id.values[0],'A','sentiment',line.strip(),'',time.strftime('%Y-%m-%d %H:%M:%S'),time.strftime('%Y-%m-%d %H:%M:%S'),'A']
        asp_key.append(row)
for file in glob.glob(asp_sent+'/*.txt'):
    name = file.split('/')[-1].split('.txt')[0]
    asp_name = name.split('_')[0]
    sent  = name.split('_')[-1].split('.')[0]
    temp_df.aspect = aspect_df.aspect_name.apply(lambda x: x.lower().strip())
    temp_df_id = aspect_df.id[temp_df.aspect == asp_name]
    f = open(file,'rb')
    lines =f.readlines()
    lines = [words for segments in lines for words in segments.replace('\n','').split('\r')]
    lines = filter(None, lines)
    for line in lines:
        row = ["",1,category_id,temp_df_id.values[0],'AS','sentiment',line.strip(),sent.strip(),time.strftime('%Y-%m-%d %H:%M:%S'),time.strftime('%Y-%m-%d %H:%M:%S'),'A']
        asp_key.append(row)    
for file in glob.glob(domain+'/*.txt'):
    name = file.split('/')[-1].split('.txt')[0]
    asp_name = name.split('_domain_')[0]
    sent  = name.split('_domain_')[-1].split('.')[0]
    temp_df.aspect = aspect_df.aspect_name.apply(lambda x: x.lower().strip())
    temp_df_id = aspect_df.id[temp_df.aspect == asp_name]

    f = open(file,'rb')
    lines =f.readlines()
    lines = [words for segments in lines for words in segments.replace('\n','').split('\r')]
    lines = filter(None, lines)
    if len(lines) <1:
        if sent.strip() == 'negative':
            line ="less"
        else:
            line ="more"
        row = ["",1,category_id,temp_df_id.values[0],'DW','sentiment',line.strip(),sent.strip(),time.strftime('%Y-%m-%d %H:%M:%S'),time.strftime('%Y-%m-%d %H:%M:%S'),'A']
        asp_key.append(row)
    for line in lines:
        row = ["",1,category_id,temp_df_id.values[0],'DW','sentiment',line.strip(),sent.strip(),time.strftime('%Y-%m-%d %H:%M:%S'),time.strftime('%Y-%m-%d %H:%M:%S'),'A']
        asp_key.append(row)
all_asp= pd.DataFrame(asp_key)
all_asp.columns =['id','account_id','category_id','aspect_id','lexicon_type','classification_type','keyword','sentiment_type','created_date','modified_date','status']
all_asp.to_csv('aspect_keyword_map_{}_{}.csv'.format(category_id,category),index=False)
