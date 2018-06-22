import csv
import sys,io,re,os
import pandas as pd

#Input: enixta treemap file, google nlp file
#Output: 4  Files: 1) Where enixta and google are matching, 2) Where enixta and google are not matching 3) combined file of both match and non-match 4) Snippets for which google sentences are not found
google_nlp = pd.read_csv('google_wm_output.csv',quotechar="\"",sep="~",escapechar='\\',names=['source_product_id','source_review_id','sentence','sentiment_score','sentiment_magnitude'])
sent=[]
for row in google_nlp.to_dict("records"):
#    start = row["review_text"].find(row["sentence"])
#    end = start + len(row["sentence"]) - 1
#    row["start_index"]=start
#    row["end_index"]=end
    if float(row["sentiment_score"]) > 0.25:
        row["sentiment"]="positive"
    elif float(row["sentiment_score"]) < -0.25:
        row["sentiment"]="negative"
    else:
        row["sentiment"]="neutral"
    sent.append(row)
enixta = pd.read_csv('flipkart_3_WM_output_removed_overlap_and_errors.txt', quotechar="\"",sep="," )
google_nlp=pd.DataFrame.from_dict(sent)
review_ids = list(enixta.source_review_id.unique())
error=[]
full_snippets=[]
output=[]
row3=[]
for review_id in review_ids:   
    reviews_google=google_nlp[google_nlp.source_review_id == review_id]
    reviews_enixta=enixta[enixta.source_review_id == review_id]
    for row1 in reviews_enixta.to_dict("records"):
        for row2 in reviews_google.to_dict('records'):
            x = re.sub(r'([^\s\w]|_)+', ' ',row1["sentiment_text"] ).lower().replace('  ',' ').strip()
            x = row1["sentiment_text"].replace('\n',' ').replace('  ',' ').replace('  ',' ')
            y = re.sub(r'([^\s\w]|_)+', ' ',row2["sentence"] ).lower().replace('  ',' ').strip()
            y = row2["sentence"].replace('\n',' ').replace('  ',' ').replace('  ',' ')
            if x in y:
#            if row1['start_index_sentiment'] >= row2['start_index'] and row1['end_index_sentiment'] <= row2['end_index']: 
                row1['sentiment_type'] = row1['sentiment_type'].strip()
                if row2['sentiment'] == row1['sentiment_type']:
                    output.append(row1)
                    full_snippets.append(row1)
                    break
                else:                     
                    if row1['confidence_score'] >= 0.95:                       
                        output.append(row1)
                        full_snippets.append(row1)
                    else:
                        error.append(row1)
                        full_snippets.append(row1)

enixta_right=pd.DataFrame.from_dict(output)
enixta_error=pd.DataFrame.from_dict(error)
enixta_full=pd.DataFrame.from_dict(full_snippets)
#1)
enixta_right.to_csv("category_output_right.csv",index=False)
#2)
enixta_error.to_csv("category_error_log.csv",index=False)
#3)
enixta_full.to_csv("category_full_polarity.csv",index=False)
################## To find the not_found snippets in google sentences
review_google = list(google_nlp.sentence)
not_found_snippets = list(set(list(enixta.sentiment_text)) - set(enixta_full["sentiment_text"]))
sent1= list(not_found_snippets)
#4)
try:
    os.remove("category_not_found.csv")
except:
    for sente in sent1:  
        reviews1 =enixta[enixta.sentiment_text == sente]
        reviews1.to_csv("category_not_found.csv",mode='a', index=False)
        
