# coding: utf-8

import re
import os
import time
import logging
import argparse
from collections import namedtuple
from gensim.utils import smart_open, to_utf8, tokenize
from utils import *
from nltk.tokenize import word_tokenize
import math


def fine_grained_sentiment_search(phrase_dir, reviews_file,
                                  output_file, window,
                                  review_field, delimiter,
                                  negation_words_file):
    """ Tags the reviews with the found aspects and sentiment words using
    pre-existing set of keywords for aspects and sentiments. Sentiment class
    is divided into 5 classes which are most-positive, positive, neutral,
    negative and most-negative .
    :param phrase_dir: directory containing aspects and sentiments phrases
    :param reviews_file: reviews file from scrapper output after pre-processing
    :param output_file: path of the output file
    :param window: window size for max length of phrase
    :param review_field: column num of the review in input review file
    :param delimiter: delimiter for col separation.
    """

    # Opening the review input file and output file
    infile = smart_open(reviews_file, 'rb')
    outfile = smart_open(output_file, 'wb')
    #tester_file = smart_open("/Users/enixtainnovationspvtltd/Documents/tags_aspects_test.txt","wb")
    #tester_file_2 = smart_open("/Users/enixtainnovationspvtltd/Documents/tags_aspects_test_2.txt","wb")
    #tester_file_3 = smart_open("/Users/enixtainnovationspvtltd/Documents/tags_aspects_test_3.txt","wb")
    #tester_file_4 = smart_open("/Users/enixtainnovationspvtltd/Documents/tags_aspects_test_4.txt","wb")

    # reading the phrases from the phrase_dir in tree based way
    phrase_dict, global_dict = dict_of_phrases(phrase_dir=phrase_dir)

    # reading the negation words from the negations file
    negation_words_list = load_negation_words(negation_words_file=
                                              negation_words_file)


    # Iterating every line of the review file
    #print >> tester_file_3, phrase_dict
    #print >> tester_file_4, global_dict
    print >> outfile, "source~source_review_id~source_product_id~product_name~review_date~star_rating~verified_user~reviewer_name~review_url~sentence~aspect~sentiment~aspect_keyword~sentiment_keyword"

    for line_num, line in enumerate(infile):
        #print >> tester_file_2, str(line_num) + "~" + str(line)
        try:
            # displaying the progress
            if line_num % 1000 is 0:
                logger.info("Processed %i lines" % line_num)

            # writing the line to outfile
            #print >> outfile, to_utf8(line.strip()), delimiter,
            #print >> 1-2493~30f416eb-4b74-48ae-a2ac-7d04679eaec7~TVSE3BZFJYBRHPCH~~2015-12-16 01:29:08~5~~~~not better than other this few pries
            #print >> tester_file, to_utf8(line.strip()), "|",

            # removing special symbols, spaces in sentence
            sentence = sentence_preprocessing(line=line,
                                              review_field=review_field,
                                              delimiter=delimiter)

            # tokenizing the sentence to words and converting
            # the returned generator to list
            #toks1 = list(tokenize(sentence, lowercase=True))
            #using nltk tokenizer instead of gensim, because gensim was dropping non alphabetic characters
            toks1 = word_tokenize(sentence.lower())
            temp_dict = dict()

            if len(toks1) > 1:
                # From the review, extracting all the valid phrases
                # and do a super phrase check on them
                temp_dict = valid_phrase_search(tokens=toks1,
                                                all_phrases_dict=global_dict,
                                                window=window)

            # Keeping only super-phrases from the phrases found
            line_phrase_dict = check_super_phrase(phrase_list=temp_dict.keys(),
                                                  phrase_dict=temp_dict,
                                                  window=window)

            # remove the side overlapping phrases.
            toks, line_phrase_list, string = filter_side_overlapping_phrases(line_phrase_dict, tokens=toks1)

            # Detecting the negation in a sentence
            # If any of the below negative words occurs in a sentence, the next
            # (count=5) words are prefixed with NEG_ prefix in transformed string

            # making a regular expression for negation words using OR operator.
            # see python 2.7 reg exp manual for more details
            neg_words_regex = '|'.join(r'%s' % neg_word for neg_word in negation_words_list)
            #neg_words_regex = 'not | never | no | could have | .....'

            # making word boundaries and using blocking( ?: ) group for making regex
            regex = r'\b' + r'(?:%s)' % neg_words_regex + r'\b' + r'[\w\s_]+'

            # replacing the words after the negation words by "NEG_"
            # Please look into the below stackoverflow answer for more details
            # http://stackoverflow.com/questions/23384351/
            # how-to-add-tags-to-negated-words-in-strings-that-follow-not-no-and-never
            transformed = re.sub(regex,
                                 lambda match: re.sub(r'(\s+)(\w+)', r'\1NEG_\2', match.group(0), count=7),
                                 string)

            transformed2 = re.sub(r'[\w\s_]+\b(?:except)\b',
                                  lambda match: re.sub(r'(\s*)(\w+)', r'\1NEG_\2', match.group(0), count=5),
                                  transformed)

            transformed_tokens = transformed2.split()

            # Search for the corresponding phrase in the aspect and sentiment category
            valid_tuples = list()
            tag_tuple = namedtuple("tag_tuple", ['category', 'class_name', 'keyword', 'pos_index'])

            # line_phrase_list contains the tags identified for the review

            for tag in line_phrase_list:
                #print tag

                for category in phrase_dict:
                    #for sentiments in fine_grained_phrases
                    #for aspects in fine_grained_phrases
                    #for aspects_sentiments in fine_grained_phrases
                    tag_underscore = tag.replace(" ", "_")
                    tag_new = tag_underscore
                    #print tag_new

                    # If we detect negations in transformed tokens,
                    # then include "NEG_" string at the start of the corresponding phrase
                    if "sentiments" == category or "aspects-sentiments" == category:
                        if 'NEG_' + tag_underscore in transformed_tokens:
                            tag_new = 'NEG_' + tag_underscore

                    for cat_type in phrase_dict[category]:
                        #for positive in phrase_dict[sentiments]
                        #for camera in phrase_dict[aspects]
                        #for camera_positive in phrase_dict[aspects_sentiments]
                        if tag in phrase_dict[category][cat_type]:
                            #print phrase_dict[category][cat_type]

                            # Dealing with the case of "aspects-sentiments" directory
                            # In the aspects-sentiments category,
                            # we are very sure of the phrases that go with them.
                            # So we write them to file.
                            if category == "aspects-sentiments":
                                #cat_type = camera_positive
                                aspect_type = cat_type.split("_")[0]
                                sent_type = cat_type.split("_")[1]

                                # Changing the polarity of the sentiment due to negation detection
                                if tag_new == 'NEG_' + tag_underscore:
                                    sent_type = change_sentiment_polarity(sent_type)

                                aspect_tuple = (category.split("-")[0], aspect_type, tag_new)
                                #aspect_tuple = (aspects, camera, 'good camera')
                                sent_tuple = (category.split("-")[1], sent_type, tag_new)
                                #sent_tuple = (sentiments, positive, 'good camera')

                                # writing the aspect and sentiment tuple to file
                                print >> outfile, to_utf8(line.strip()), delimiter, aspect_tuple[1], delimiter, sent_tuple[1], delimiter, aspect_tuple[2], delimiter, sent_tuple[2]
                                #print >> outfile, ((aspects, camera, 'good camera'), (sentiments, positive, 'good camera'))
                                #print >> tester_file, "ding ding"
                                #print >> tester_file, to_utf8(line.strip()), delimiter, "|",

                                #print outfile, (aspect_tuple, sent_tuple), delimiter,

                            # For "aspects" and "sentiments" class of keywords
                            else:
                                print "here: " + category
                                tag = tag.replace(" ", "_")

                                try:
                                    tag_index = toks.index(tag)
                                except ValueError:
                                    print "line is {}".format(line_num)

                                # Changing the polarity of the sentiment due to negation detection
                                if tag_new == 'NEG_' + tag_underscore:
                                    #above we change tag_new to NEG_ + tag_underscore for sentiments and aspect-sentiments
                                    #can be "camera" or "positive" or "negative"....
                                    #print "cat_type" + cat_type
                                    cat_type = change_sentiment_polarity(cat_type)


                                # creating namedtuple to to stores keyword information
                                valid_tuples.append(tag_tuple(category, cat_type, str(tag_new), tag_index))
                                #print valid_tuples
            #print >> tester_file, valid_tuples
            #print >> tester_file, valid_tuples[1:2]
                        #print valid_tuples

            # sorting the list by pos_index
            sorted_aspect_sent_list = sorted(valid_tuples, key=lambda x: x.pos_index)
            #print sorted_aspect_sent_list

            for k, aspect_tuple in enumerate(sorted_aspect_sent_list):
                if aspect_tuple.category == 'aspects':
                    #print >> tester_file_2, aspect_tuple

                    #print >> tester_file, aspect_tuple, "|",
                    # finding the nearest sent tuple (if exists)
                    sent_tuple = min(sorted_aspect_sent_list,
                                     key=lambda x: abs(x.pos_index - aspect_tuple.pos_index) if (
                                         x.category == 'sentiments') else 1000)

                    # writing the aspect and sentiment tuple to file
                    if sent_tuple.category == "sentiments":
                        # Added the condition to avoid very long sentences
                        if math.fabs(aspect_tuple[-1] - sent_tuple[-1]) < 15:
                            print >> outfile, to_utf8(line.strip()), delimiter, aspect_tuple[1],delimiter, sent_tuple[1], delimiter, aspect_tuple[2], delimiter, sent_tuple[2]
                            #print >> outfile, (aspect_tuple[0:-1], sent_tuple[0:-1]), delimiter,
                            #print >> tester_file_2, aspect_tuple[0:-1], sent_tuple[0:-1]
                            #print >> aspect_tuple[0:-1], sent_tuple
        except:
            print "skipped line..."
            print "line_number: " + str(line_num)


        #print >> outfile, '\n',
        #print >> tester_file, "\n",
        #print >> tester_file, '\n'

    # closing the files
    infile.close()
    outfile.close()

# ##############################################################################


if __name__ == "__main__":
    parser = argparse.ArgumentParser("computes the aspect and "
                                     "sentiment tags for reviews")
    parser.add_argument('--window',
                        type=int,
                        help='look at the string length of '
                             'these words for possible candidate phrase')

    parser.add_argument('--logging_filename',
                        type=str,
                        help='filename for logging the events')

    parser.add_argument('--output_file',
                        type=str,
                        help='file to save the sentence splitted reviews')

    parser.add_argument('--reviews_file',
                        type=str,
                        help='path of input reviews file which '
                             'has one review per line')

    parser.add_argument('--phrase_dir',
                        type=str,
                        help='phrases directory path')

    parser.add_argument('--negations_file',
                        type=str,
                        help='negation inducing words file path')

    parser.add_argument('--review_field',
                        type=int,
                        help='column field number of the reviews')

    parser.add_argument('--delimiter',
                        type=str,
                        help='delimiter used in the input reviews file')

    args = parser.parse_args()

    ## Set up a file handler for the log
    handler = logging.FileHandler(args.logging_filename, mode='w')
    FORMAT = '%(asctime)s : %(levelname)s : %(message)s'

    ## Create the logger, add the file handler, and set the level to 'info'.
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    ## Set the formatter in handler
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)

    ## add the handlers to the logger
    logger.addHandler(handler)

    ## Start the timer
    start_time = time.time()

    # Calling the fine grained sentiment search function
    fine_grained_sentiment_search(phrase_dir=args.phrase_dir,
                                  reviews_file=args.reviews_file,
                                  output_file=args.output_file,
                                  window=args.window,
                                  review_field=args.review_field,
                                  delimiter=args.delimiter,
                                  negation_words_file=args.negations_file)

    # Writing the completion time to logger
    logger.info(time.time() - start_time)
