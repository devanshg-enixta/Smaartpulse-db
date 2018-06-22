CREATE TABLE `reviews_temp` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `category_id` bigint(20) NOT NULL,
  `file_id` bigint(20) NOT NULL,
  `review_id` varchar(255) NOT NULL DEFAULT '',
  `product_id` varchar(255) NOT NULL DEFAULT '',
  `review_title` varchar(1024) NULL DEFAULT '',
  `star_rating` varchar(255) COLLATE utf8_bin DEFAULT NULL,
  `review_text` text DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;


CREATE TABLE `sentiment_output_temp` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `file_id` bigint(20) NOT NULL,
  `review_id` varchar(255) NOT NULL DEFAULT '',
  `aspect_id` varchar(255) DEFAULT NULL,
  `sentiment_type` varchar(255) DEFAULT NULL,
  `sentiment_text` varchar(999) DEFAULT NULL,
  `start_index` int(11) DEFAULT NULL,
  `end_index` int(11) DEFAULT NULL,
  `start_index_partial_review` int(11) DEFAULT NULL,
  `end_index_partial_review` int(11) DEFAULT NULL,
  `confidence_score` double(5,2) DEFAULT NULL,
  `partial_review_text` text DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;



CREATE TABLE `source_reviews` (
  `file_id` bigint(10) NOT NULL,
  `product_id` varchar(255) COLLATE utf8_bin DEFAULT NULL,
  `product_name` varchar(999) COLLATE utf8_bin DEFAULT NULL,
  `review_id` varchar(255) COLLATE utf8_bin NOT NULL,
  `reviewer_name` varchar(255) COLLATE utf8_bin DEFAULT NULL,
  `star_rating` varchar(255) COLLATE utf8_bin DEFAULT NULL,
  `review_date` varchar(255) COLLATE utf8_bin DEFAULT NULL,
  `review_title` varchar(1000) COLLATE utf8_bin DEFAULT NULL,
  `category_id` bigint(10) NOT NULL,
  `review_url` varchar(255) COLLATE utf8_bin DEFAULT NULL,
  `review_text` blob NOT NULL,
  UNIQUE KEY `source_reviews_idx1` (`file_id`,`product_id`,`review_id`) COMMENT 'Index for unique key',
  KEY `source_reviews_file_fk` (`file_id`) COMMENT 'FK Index. Reference file_uploads',
  KEY `source_reviews_category_fk` (`category_id`) COMMENT 'FK Index. Reference file_uploads',
  KEY `review_id_idx` (`review_id`),
  CONSTRAINT `source_reviews_category_fk` FOREIGN KEY (`category_id`) REFERENCES `category` (`id`),
  CONSTRAINT `source_reviews_file_fk` FOREIGN KEY (`file_id`) REFERENCES `file_uploads` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin


import pandas as pd
import itertools
from itertools import *
import os,re,sys
keyword_table_name = "keywords_{}".format(file_id)
keyword_table_query = "CREATE TABLE `{}` " \
                    "`file_id` bigint(20) NOT NULL, " \
                    "`category_id` varchar(255) DEFAULT NULL, " \
                    "`keywords` varchar(255) DEFAULT NULL, " \
                    "ENGINE=InnoDB AUTO_INCREMENT=1 " \
                    "DEFAULT CHARSET=utf8;".format(keyword_table_name)

lexicon_db.execute(keyword_table_query)
file = open(file_name.txt,'wb')
final_output = pd.DataFrame()
asp_keywords = file.readlines()
asp_keywords = [words for segments in asp_keywords for words in segments.replace('\n','').split('\r')]
asp_keywords = filter(None, asp_keywords)
final_output['keywords'] = asp_keywords
final_output['file_id'] = file_id
final_output['category_id'] = category_id
final_output = final_output.drop_duplicates()
final_output.to_sql(keyword_table_name, lexicon_db, if_exists='append',index=False)
Lets have the below table desc : 
CREATE TABLE `aspect_keywords` (
 `id` bigint(10) NOT NULL AUTO_INCREMENT,
 `file_id` bigint(20)  NOT NULL COMMENT 'Holds id of the category File!',
 `category_id` varchar(255) NOT NULL COMMENT 'Holds the id of the category',
 `aspect_keywords` timestamp NOT NULL COMMENT 'Current Aspect Keywords for that category',
 `created_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Current Aspect Keywords for that category',
 `modified_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modified Date. Default Value: Current timestamp',
 `status` varchar(1) DEFAULT 'A' COMMENT 'Status. A for active. I for Inactive',
 PRIMARY KEY (`id`)
)
Correction : Lets name the table as category_keywords , cuz there is already a table called aspect_keywords_map, will be confusing. Rename the column also as just keywords  
CREATE TABLE `category_keywords` (
 `id` bigint(10) NOT NULL AUTO_INCREMENT,
 `file_id` bigint(20) NOT NULL COMMENT 'Holds id of the category File!',
 `category_id` varchar(255) NOT NULL COMMENT 'Holds the id of the category',
 `keywords` varchar(255) NOT NULL COMMENT 'Current Aspect Keywords for that category',
 `created_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Current Aspect Keywords for that category',
 `modified_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modified Date. Default Value: Current timestamp',
 `status` varchar(1) DEFAULT 'A' COMMENT 'Status. A for active. I for Inactive',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=817 DEFAULT CHARSET=latin1;




