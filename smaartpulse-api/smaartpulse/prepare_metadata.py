import os, sys, shutil
import pandas as pd
import unicodecsv as csv

def create_dir(path):
    try:
        os.mkdir(path)
    except:
        print "Problem creating the directory. Permission denied!"


class LexiconMetadata(object):
    def __init__(self, root_dir, file_id, client_id, category_id, db, local_db, input_file, final_dir, classification_type='sentiment'):
        self.root_dir = root_dir
        self.file_id = file_id
        self.client = client_id
        self.con = db
        self.local_db = local_db
        self.category_id = category_id
        self.classification_type = classification_type
        self.input_file = input_file
        #self.input_df = input_df
        self.final_dir = final_dir

        # Required variables
        self.client_dir = None
        self.data_annotation_dir = None
        self.domain_words_dir = None
        self.input_dir = None
        self.output_dir = None
        self.log_dir = None
        self.category = None
        self.final_file = None

        # Final output parameter
        self.output = None

        # Flag to know the presence of required data
        self.status = True
        self.status_msg = None

        # Required queries
        self.aspects_query = "select id as aspect_id from category_aspect " \
                             "where category_id={};".format(self.category_id)
        self.aspect_lexicon_query = "select aspect_id, keyword from aspect_keyword_map " \
                                    "where classification_type='{}' " \
                                    "and lexicon_type='A' " \
                                    "and category_id = {} " \
                                    "and account_id={};".format(self.classification_type,
                                                                self.category_id,
                                                                self.client)
        self.aspect_sentiment_lexicon_query = "select aspect_id, keyword, sentiment_type from aspect_keyword_map " \
                                              "where classification_type='{}' " \
                                              "and lexicon_type='AS' " \
                                              "and category_id={} " \
                                              "and account_id={};".format(self.classification_type,
                                                                          self.category_id,
                                                                          self.client)

        self.sentiment_lexicon_query = "select keyword, sentiment_type from word_sentiment_map " \
                                              "where classification_type='{}';".format(self.classification_type)

        self.domain_words_query = "select aspect_id, keyword, sentiment_type from aspect_keyword_map " \
                                  "where classification_type='{}' " \
                                  "and lexicon_type='DW' " \
                                  "and category_id={} " \
                                  "and account_id={};".format(self.classification_type,
                                                              self.category_id,
                                                              self.client)

        # Prepare a local db table to hold the review data
        self.review_table_name = 'reviews_temp_{}'.format(file_id)
        self.review_table_query = "CREATE TABLE {} (`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, " \
                                  "`category_id` bigint(20) NOT NULL, `file_id` bigint(20) NOT NULL, " \
                                  "`review_id` varchar(255) NOT NULL DEFAULT '', " \
                                  "`product_id` varchar(255) NOT NULL DEFAULT '', " \
                                  "`review_title` varchar(1024) NULL DEFAULT '', " \
                                  "`star_rating` varchar(255) COLLATE utf8_bin DEFAULT NULL," \
                                  "`review_text` text DEFAULT NULL," \
                                  "PRIMARY KEY (`id`)) " \
                                  "ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;".format(self.review_table_name)

        # Prepare the table to hold the process data
        self.snippet_table_name = "smartpulse_snippets_{}".format(file_id)
        self.snippet_table_query = "CREATE TABLE `{}` " \
                                   "(`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, " \
                                   "`file_id` bigint(20) NOT NULL, " \
                                   "`review_id` varchar(255) NOT NULL DEFAULT '', " \
                                   "`aspect_id` varchar(255) DEFAULT NULL, " \
                                   "`sentiment_type` varchar(255) DEFAULT NULL, " \
                                   "`sentiment_text` varchar(999) DEFAULT NULL, " \
                                   "`start_index` int(11) DEFAULT NULL, " \
                                   "`end_index` int(11) DEFAULT NULL, " \
                                   "`start_index_partial_review` int(11) DEFAULT NULL, " \
                                   "`end_index_partial_review` int(11) DEFAULT NULL," \
                                   "`confidence_score` double(5,2) DEFAULT NULL, " \
                                   "`partial_review_text` text DEFAULT NULL," \
                                   "PRIMARY KEY (`id`)) " \
                                   "ENGINE=InnoDB AUTO_INCREMENT=1 " \
                                   "DEFAULT CHARSET=utf8mb4;".format(self.snippet_table_name)


        self.get_category()
        self.aspect_ids = pd.read_sql(self.aspects_query, con=self.con).aspect_id.unique()
        self.prepare_directory()
        self.create_lexicon_conf()
        self.prepare_inputfile()
        self.prepare_input_db()
        #self.prepare_output_db()

    def prepare_input_db(self):
        self.local_db.execute("Drop table if exists {};".format(self.review_table_name))
        self.local_db.execute(self.review_table_query)
        # try:
        #   reviews = pd.read_csv(self.input_file, sep='~', quoting=csv.QUOTE_ALL)
        # except:
        #print "pass"
        try:
          reviews = pd.read_csv(self.input_file, quoting=csv.QUOTE_ALL,encoding='latin')
        except:
          reviews = pd.read_csv(self.input_file, quoting=csv.QUOTE_ALL,quotechar="\"",escapechar='\\')
        try:
          reviews =reviews.rename(columns={"productId":"product_id"})
          #print reviews.columns
        except:
            pass
        try:
          reviews =reviews.rename(columns={"text":"review_text"})
        except:
            pass
        try:
          reviews =reviews.rename(columns={"id":"review_id"})
        except:
            pass
        try:    
          reviews =reviews.rename(columns={"rating":"star_rating"})
        except:
            pass
        try:
          reviews =reviews.rename(columns={"title":"review_title"})
        except:
            pass
        #
        reviews['review_text'] = reviews['review_text'].apply(lambda x: x.encode('ascii','ignore').decode('ascii'))
        reviews['review_text'] = reviews['review_text'].astype(str)

        reviews.rename(columns={'review_tag':'review_title', 'source_review_id':'review_id',
                                'source_product_id': 'product_id'}, inplace=True)
        reviews['category_id'] = self.category_id
        reviews['file_id'] = self.file_id
        reviews['review_text'] = reviews['review_text'].astype(str)
        cols = self.local_db.execute("desc {};".format(self.review_table_name))
        cols = [x[0] for x in list(cols) if x and x[0] not in [u'id', u'created_date', u'modified_date']]

        reviews = reviews[cols]
        reviews.to_sql(self.review_table_name, if_exists='append', con=self.local_db, index=False, chunksize=1000)

    def prepare_output_db(self):
        self.local_db.execute("Drop table if exists {};".format(self.snippet_table_name))
        self.local_db.execute(self.snippet_table_query)

    def get_category(self):
        category_name = list(self.con.execute("select category_name from category "
                                             "where id={};".format(self.category_id)))
        if not category_name:
            self.status = False
            self.status_msg = "Category id is missing in the database!"
            return
        self.category = category_name[0][0]

    def prepare_inputfile(self):
        file_name_target = self.input_file.split('/')[-1]
        shutil.copy(self.input_file, "{}/{}".format(self.input_dir, file_name_target))
        self.input_file = os.path.join(self.input_dir, file_name_target)

        self.final_file = os.path.join(self.final_dir, file_name_target)

        self.log_dir = os.path.join(self.client_dir, 'logs')
        create_dir(self.log_dir)

    def prepare_directory(self):
        self.client_dir = os.path.join(self.root_dir, 'metadata_{}_{}'.format(self.client, self.file_id))
        if os.path.isdir(self.client_dir):
            shutil.rmtree(self.client_dir)
        create_dir(self.client_dir)

        self.data_annotation_dir = os.path.join(self.client_dir, 'data_annotation')
        create_dir(self.data_annotation_dir)

        self.domain_words_dir = os.path.join(self.client_dir, 'domain_words')
        create_dir(self.domain_words_dir)

        self.input_dir = os.path.join(self.client_dir, 'input_folder')
        create_dir(self.input_dir)

        self.output_dir = os.path.join(self.client_dir, 'output_folder')
        create_dir(self.output_dir)

    def create_lexicon_conf(self):
        aspect_lexicon_phrases = pd.read_sql(self.aspect_lexicon_query, con=self.con)
        if aspect_lexicon_phrases.empty:
            self.status = False
            self.status_msg = "No Aspect lexicon data found for category {}.".format(self.category)
            return

        aspect_phrase_dir = os.path.join(self.data_annotation_dir, 'aspects')
        create_dir(aspect_phrase_dir)
        for aspect_id in self.aspect_ids:
            aspect_lex_phrases = aspect_lexicon_phrases[aspect_lexicon_phrases.aspect_id==aspect_id]
            aspect_lex_phrases[['keyword']].to_csv(aspect_phrase_dir+'/{}.txt'.format(aspect_id), header=False,
                                                   index=False, encoding='utf8')

        aspect_sent_lexicon_phrases = pd.read_sql(self.aspect_sentiment_lexicon_query, con=self.con)
        if aspect_sent_lexicon_phrases.empty:
            self.status = False
            self.status_msg = "No Aspect Sentiment lexicon data found for category {}.".format(self.category)
            return

        aspect_sent_phrase_dir = os.path.join(self.data_annotation_dir, 'aspects-sentiments')
        sentiments = list(aspect_sent_lexicon_phrases.sentiment_type.unique())
        create_dir(aspect_sent_phrase_dir)
        for aspect_id in self.aspect_ids:
            for sent in sentiments:
                sent = sent.strip().replace(' ','-')
                aspect_sent_lex_phrases = \
                    aspect_sent_lexicon_phrases[(aspect_sent_lexicon_phrases.aspect_id == aspect_id)&
                                                (aspect_sent_lexicon_phrases.sentiment_type==sent)]
                aspect_sent_lex_phrases[['keyword']].to_csv(aspect_sent_phrase_dir + '/{}_{}.txt'.format(aspect_id,
                                                                                            sent.replace(' ','-')),
                                                            header=False,
                                                            index=False,
                                                            encoding='utf8')

        sentiment_lexicon_phrases = pd.read_sql(self.sentiment_lexicon_query, con=self.con)
        if sentiment_lexicon_phrases.empty:
            self.status = False
            self.status_msg = "No Sentiment lexicon data found for category {}.".format(self.category)
            return

        sent_phrase_dir = os.path.join(self.data_annotation_dir, 'sentiments')
        create_dir(sent_phrase_dir)
        sents = list(sentiment_lexicon_phrases.sentiment_type.unique())
        for sent in sents:
            sent = sent.strip().replace(' ', '-')
            sentiment_lex_phrases = sentiment_lexicon_phrases[sentiment_lexicon_phrases.sentiment_type == sent]
            sentiment_lex_phrases[['keyword']].to_csv(sent_phrase_dir + '/{}.txt'.format(sent), header=False,
                                                      index=False,
                                                      encoding='utf8')

        domain_word_lexicon_phrases = pd.read_sql(self.domain_words_query, con=self.con)
        if domain_word_lexicon_phrases.empty:
            self.status = False
            self.status_msg = "No Domain Words data found for category {}.".format(self.category)
            return

        domain_word_phrase_dir = os.path.join(self.domain_words_dir,'sentiments')
        create_dir(domain_word_phrase_dir)
        sentiments = list(domain_word_lexicon_phrases.sentiment_type.unique())
        for aspect_id in self.aspect_ids:
            for sent in sentiments:
                sent = sent.strip().replace(' ', '-')
                domain_word_lex_phrases = \
                    domain_word_lexicon_phrases[(domain_word_lexicon_phrases.aspect_id == aspect_id) &
                                                (domain_word_lexicon_phrases.sentiment_type == sent)]
                domain_word_lex_phrases[['keyword']].to_csv(domain_word_phrase_dir + '/{}_domain_{}.txt'.format(
                                                    aspect_id, sent.replace(' ', '-')),
                                                            header=False,
                                                            index=False,
                                                            encoding='utf8')