import re
import glob
import os
from itertools import *
from nltk.tokenize import word_tokenize
from nltk.util import ngrams
import unicodecsv as csv
import pandas as pd


def get_ngrams(text):
    list1 = []
    for i in range(0, len(text.split(' '))):
        n_grams = ngrams(word_tokenize(text), i)
        list1.append([' '.join(grams) for grams in n_grams])
    list1 = list(chain.from_iterable(list1))
    return list1


def correct_snippets(data_file, config):

    data_annotation_dir = config.data_annotation_dir
    try:
        enixta_right1 = pd.read_csv(data_file, sep='~', quoting=csv.QUOTE_ALL)
    except:
        try:
            enixta_right1 = data_file
        except:
            enixta_right1 = pd.read_csv(data_file, quoting=csv.QUOTE_ALL)
    enixta_right1 = enixta_right1[~(enixta_right1.sentiment_text.isnull())]

    enixta_right1.aspect = enixta_right1.aspect.map(str)
    input_cols = enixta_right1.columns.values
    asp = list(enixta_right1.aspect.unique())
    out = []
    file = os.path.join(data_annotation_dir, 'sentiments/positive.txt')
    f = open(file, 'r+')
    keyword = f.readlines()
    pos = [i.replace('\n', '').split('\r', 1)[0] for i in keyword]
    file = os.path.join(data_annotation_dir, 'sentiments/negative.txt')
    f = open(file, 'r+')
    keyword = f.readlines()
    neg = [i.replace('\n', '').split('\r', 1)[0] for i in keyword]
    sentiment = list(chain(pos, neg))
    for l in asp:
        t = enixta_right1[enixta_right1.aspect == l]
        #     if l == 'value_for_money':
        #         l = 'value-for-money'
        file = os.path.join(data_annotation_dir, 'aspects', l + '.txt')
        f = open(file, 'r+')
        keyword = f.readlines()
        keyword = [i.replace('\n', '').replace('\t', '').replace('  ', ' ') for i in keyword]
        list_ = [words for segments in keyword for words in segments.split('\r')]
        key = []
        for file in glob.glob(os.path.join(data_annotation_dir, 'aspects-sentiments', l + '*')):
            f = open(file, 'r+')
            x = f.readlines()
            key.append(x)
        key = list(chain.from_iterable(key))
        key = [i.replace('\n', '') for i in key]
        list_asp_sent = [words for segments in key for words in segments.split('\r')]
        for line_num,row in enumerate(t.to_dict('records')):
            flag = 0
            row['STATUS']  =5
            row['WHY'] = 'SC'
            # try:

            #################################################################
            # Splitting by i,but,plz,pls,and,*, i m, etc.   both sentiment text and review text
            a = row['sentiment_text'].encode('ascii','ignore').decode('ascii').lower()
            sp = re.compile(re.escape(' i '))
            a = re.sub(sp,".",a)
            sp = re.compile(re.escape(' but '))
            a = re.sub(sp,".",a)
            sp = re.compile(re.escape(' plz '))
            a = re.sub(sp,".",a)
            sp = re.compile(re.escape(' pls '))
            a = re.sub(sp,".",a)
            sp = re.compile(re.escape(' please '))
            a = re.sub(sp,".",a)
            sp = re.compile(re.escape(' * '))
            a = re.sub(sp,".",a)
            sp = re.compile(re.escape(' i m '))
            a = re.sub(sp,".",a)
            sp = re.compile(re.escape(' i am '))
            a = re.sub(sp,".",a)
            sp = re.compile(re.escape(" i'm "))
            a = re.sub(sp,".",a)

            sp = re.compile(re.escape(" & "))
            a = re.sub(sp,".",a)
            sp = re.compile(re.escape("///"))
            a = re.sub(sp,".",a)


            B = row['review_text'].encode('ascii','ignore').decode('ascii')
            b = B.lower().replace('       ',' ')
            #print B,"ybrghvdjfhgvjksdfnxcm"
            sp = re.compile(re.escape(' i m '))
            b = re.sub(sp,".",b)
            sp = re.compile(re.escape(' i am '))
            b = re.sub(sp,".",b)
            sp = re.compile(re.escape(" i'm "))
            b = re.sub(sp,".",b)

    #         sp = re.compile(re.escape(' i '))
    #         b = re.sub(sp,".",b)
            sp = re.compile(re.escape(' plz '))
            b = re.sub(sp,".",b)
            sp = re.compile(re.escape(' pls '))
            b = re.sub(sp,".",b)
            sp = re.compile(re.escape(' please '))
            b = re.sub(sp,".",b)

            sp = re.compile(re.escape(' but '))
            b = re.sub(sp,".",b)
            sp = re.compile(re.escape(' so '))
            b = re.sub(sp,".",b)
            sp = re.compile(re.escape(' * '))
            b = re.sub(sp,".",b)

            sp = re.compile(re.escape(' & '))
            b = re.sub(sp,",",b)
            sp = re.compile(re.escape('///'))
            b = re.sub(sp,",",b)
           

            if '/' in a and '/' in b:
                sent_text = re.split('[,\t!;\r?\n]', a)
                sent_text = list(filter(None, sent_text))
                p_text = re.split('[,\t!;\r?\n]', b)
                p_text = list(filter(None, p_text))
            else:
                sent_text = re.split('[\t.,!;\r?\n]', a)
                sent_text = list(filter(None, sent_text))
                p_text = re.split('[,\t.!;\r?\n]', b)
                p_text = list(filter(None, p_text))  
                sent =""

                for sent in sent_text:
                    sent = sent.strip()
                    ntxt = get_ngrams(sent)
                    match = set(ntxt) & set(list_)
                    match = sorted(match, key=lambda k: ntxt.index(k))
                    # chceking if any aspect is present in any of the splitted snippets of sentiment text
                    # like good and camera ----> ['good','camera'] and aspect found in 2nd element
                    if any(match):
                        flag = 1
                        sent_match = set(ntxt) & set(sentiment)
                        sent_match = sorted(sent_match, key=lambda k: ntxt.index(k))
                        # check if any sentiment is related to the aboe aspect in the same element if it is then pass elese
                        # correct the snetiment with the review text using same logic.
                        if not any(sent_match) :
                            for pt in p_text:
                                pt = pt.strip()
                                if sent in pt:
                                    if abs(len(pt.split(' ')) - len(row['sentiment_text'].split(' '))) <= 5 or len(
                                            row['sentiment_text'].split(' ')) < 3:
                                        row['sentiment_text'] = pt
                                        row['STATUS'] =5
                                        if "sentiment" in row['WHY'].lower().strip():
                                            row['WHY'] = "AS SC NS"
                                        else:
                                            row['WHY'] = "A SC NS"
                                        sent = pt
                        if len(sent.strip().split(' ')) <2:
                            for pt in p_text:
                                pt = pt.strip()
                                if sent in pt:
                                    if abs(len(pt.split(' ')) - len(row['sentiment_text'].split(' '))) <= 5 or len(
                                            row['sentiment_text'].split(' ')) < 3:
                                        row['sentiment_text'] = pt
                                        row['STATUS'] =5
                                        if "sentiment" in row['WHY'].lower().strip():
                                            row['WHY'] = "AS SC SW"
                                        else:
                                            row['WHY'] = "A SC SW"


            out.append(row.copy())


    out = pd.DataFrame(out)
    out = out.drop_duplicates()
    out = out[input_cols]
    out.aspect = out.aspect.map(int)
    return out
    # try:
    #     del out['review_text']
    # except:
    #     pass
   # out.to_csv(data_file, sep='~', index=False, quoting=csv.QUOTE_ALL, encoding='utf-8')





