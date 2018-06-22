import pandas as pd

product_source_map = pd.read_csv("Holiday_iQ-Product_Source_Map_hotels.csv")
treemap = pd.read_csv("/Users/apple/Documents/smaartpulse_required_python_scripts/data_files/2017-12-15_hotels_sentiment/final_output_folder/treemap_outputfile_2017-12-15_hotels_sentiment.txt",delimiter = '~')
reviews = pd.read_csv("/Users/apple/Documents/smaartpulse_required_python_scripts/data_files/2017-12-15_hotels_sentiment/final_output_folder/source_reviews_outputfile_2017-12-15_hotels_sentiment.txt", delimiter = '~')
summary = pd.read_csv("/Users/apple/Documents/smaartpulse_required_python_scripts/data_files/2017-12-15_hotels_sentiment/final_output_folder/summary_outputfile_2017-12-15_hotels_sentiment.txt",delimiter = '~')
treemap_map = pd.merge(treemap, product_source_map)
reviews_map = pd.merge(reviews, product_source_map)
summary_map = pd.merge(summary,product_source_map)

treemap_map['aspect'] = treemap_map[]

cols_treemap = ["source",'source_review_id','product_id_map','aspect','sentiment_text', 'review_text_tag_ind', 'start_index','end_index','confidence-score','source_product_id','treemap_name','category_id','sentiment_type']
cols_source_review = ["source",'source_review_id','source_product_id','product_name','review_date','star_rating','reviewer_id','reviewer_name','review_url','review_tag','review_text','product_id_map']

treemap_cols_new = treemap_map[cols_treemap]
source_cols_new = reviews_map[cols_source_review]
treemap_cols_new.rename(columns={'product_id_map':'product_id'},inplace= True)
source_cols_new.rename(columns={'product_id_map':'product_id'},inplace = True)

aspect_to_id_dict = {'room':1,'location':3,'staff':2,'food':6,'cleanliness':5,'vfm':4,'all':7}

summary_map.to_csv("summary_outputfile_2017-12-15_hotels_sentiment_mapped.txt", sep = '~', index = False)