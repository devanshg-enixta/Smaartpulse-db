if __name__ == "__main__":

    parser = argparse.ArgumentParser("Rearranging_columns")

    parser.add_argument('--category',
                        type=str)

    parser.add_argument('--raw_reviews_file',
    					type=str)
    parser.add_argument('output_file',
    					type=str)


#parser.add_argument('--positive',
#type=str)

    args = parser.parse_args()

    category = args.category
	input_file = args.raw_reviews_file
	outfile = args.output_file
	reviews = pd.read_csv(input_file)

	reviews.head()
	output_file = os.path.join(os.getcwd(),category + "_data" + outfile)
	#source~source_review_id~source_product_id~product_name~review_date~star_rating~verified_user~reviewer_name~review_url~review_tag~review_text

	reviews['source'] = 1
	reviews['product_name'] = ""
	reviews['review_text'] = reviews['title'] + ". " + reviews['text']
	reviews['review_url'] = ""
	reviews['review_tag'] = ""
	reviews['reviewer_name'] = ""

	reviews.rename(columns={'id':'source_review_id','productId':'source_product_id','rating':'star_rating','certifiedBuyer':'verified_user','createdOn':'review_date'},inplace=True)

	cols = ['source','source_review_id','source_product_id','product_name','review_date','star_rating','verified_user','reviewer_name','review_url','review_tag','review_text']

	reviews_new = reviews[cols]

	reviews_new.to_csv(output_file,sep='~',index=False)
	reviews_new.head()