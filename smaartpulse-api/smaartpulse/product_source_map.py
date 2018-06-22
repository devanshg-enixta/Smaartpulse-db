import pandas as pd

def product_source_mapping(product_source_map,treemap_file,source_reviews_file,summary_file, treemap_mapped_outfile, source_reviews_mapped_outfile, summary_mapped_outfile):
	product_source_map = pd.read_csv(product_source_map)
	treemap = pd.read_csv(treemap_file,delimiter = '~')
	reviews = pd.read_csv(source_reviews_file, delimiter = '~')
	summary = pd.read_csv(summary_file,delimiter = '~')
	treemap_map = pd.merge(treemap, product_source_map, on='source_product_id')
	reviews_map = pd.merge(reviews, product_source_map, on='source_product_id')
	summary_map = pd.merge(summary,product_source_map, on='source_product_id')
	#aspect_to_id_dict = {'room':1,'location':3,'staff':2,'food':6,'cleanliness':5,'vfm':4,'all':7}

	#treemap_map['aspect'] = treemap_map['aspect'].apply(lambda x:x.strip()).map(aspect_to_id_dict)

	cols_treemap = ["source",'source_review_id','product_id_map','aspect','sentiment_text', 'review_text_tag_ind', 'start_index','end_index','confidence-score','source_product_id','treemap_name','category_id','sentiment_type']
	cols_source_review = ["source",'source_review_id','source_product_id','product_name','review_date','star_rating','reviewer_id','reviewer_name','review_url','review_tag','review_text','product_id_map']

	treemap_cols_new = treemap_map[cols_treemap]
	source_cols_new = reviews_map[cols_source_review]
	treemap_cols_new.rename(columns={'product_id_map':'product_id'},inplace= True)
	source_cols_new.rename(columns={'product_id_map':'product_id'},inplace = True)



	summary_map.to_csv(summary_mapped_outfile, sep = '~', index = False)
	treemap_cols_new.to_csv(treemap_mapped_outfile,sep ='~',index = False)
	source_cols_new.to_csv(source_reviews_mapped_outfile,sep='~',index= False)
