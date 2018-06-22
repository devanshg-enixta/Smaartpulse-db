# coding: utf-8

import pandas as pd
from pandas.io import parsers
import argparse
import csv

########################################################################
# Input validation Checks
# 1. Check if required columns are there or not
# 2. Check if any null values in the required columns for ml
# 3. Check if there are any duplicate reviews existing
# 4. Check if any reviews is mapped to multiple products
#######################################################################


########################################################################
# Output validation Checks
# 1. Any duplicate row in the output
# 2. Any duplicates with aspect-sentiment logic review wise (One review should not 
#      contain both +ve and -ve snippets over an aspect!)
#######################################################################

pd.options.mode.chained_assignment = None


class ValidationError(Exception):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super(ValidationError, self).__init__(message)

        # Now for your custom code...
        # self.errors = errors


def inputfile_validation(raw_reviews, revcolmap):
    # columns = ['product_id', 'product_name', 'review_id', 'star_rating', 'review_date', 'review_title',
    #            'review_url', 'review_text']
    columns = ['review_id', 'product_id', 'review_text']
    file_cols = list(raw_reviews.columns.values)
    missing_cols = []
    for col in columns:
        if col not in file_cols:
            missing_cols.append(revcolmap[col])
    final_message = []
    if missing_cols:
        final_message.append("Missing required columns in the file! {}<br/>"
                             "This might be the error due to the wrong delimiter also. "
                             "Please make sure that your file's delimiter is ','(comma).".format(
            ','.join(missing_cols)))
    # raw_reviews.describe(include='all')
    print missing_cols
    if not (revcolmap['product_id'] in missing_cols or revcolmap['review_id'] in missing_cols
            or revcolmap['review_text'] in missing_cols):
        non_empty_cols = ['product_id', 'review_id']
        empty_cols = []
        for col in non_empty_cols:
            nulls = raw_reviews.drop_duplicates()[raw_reviews.drop_duplicates()[col].isnull()]
            if len(nulls) > 0:
                empty_cols.append("Empty {} column in rows {}".format(col, ','.join(map(str, nulls.index.values))))

        if empty_cols:
            final_message.append('\n'.join(empty_cols))

        review_counts = raw_reviews[['product_id', 'review_id']]
        review_counts['counts'] = 1
        review_counts = review_counts.groupby(['product_id', 'review_id'])['counts'].sum().reset_index(drop=False)
        duplicates = review_counts[review_counts.counts > 1]

        if not duplicates.empty:
            # print "Duplicate reviews with ids {}".format(list(duplicates.review_id))
            final_message.append("{} reviews found with duplicate review_id in the file!".format(duplicates.shape[0]))

        product_counts = raw_reviews[['product_id', 'review_id']]
        product_counts['counts'] = 1
        product_counts = product_counts.groupby(['review_id', 'product_id'])['counts'].sum().reset_index(drop=False)
        prod_duplicates = product_counts[product_counts.counts > 1]
        multiple_maps = False
        if not prod_duplicates.empty:
            multiple_maps = True
            # print "Reviews mapped to multiple products {}".format(list(prod_duplicates.review_id))
            final_message.append(
                "{} Reviews found that are mapped to multiple products!".format(prod_duplicates.shape[0]))

    if final_message:
        return {'errors': '<br/>'.join(final_message), 'messages': None}
    else:
        record_count = raw_reviews.drop_duplicates(['product_id', 'review_id']).shape[0]
        return {'errors': None,
                'messages': ['File upload is successful! Valid record count is {}.'.format(record_count)]}


# Object to validate the output file
class OutputValidation(object):
    def __init__(self, df):
        self.df = df
        self.duplicates = None

    def check_duplicates(self, cols=[]):
        init_size = self.df.shape[0]
        if not cols:
            de_dup_size = self.df.drop_duplicates().shape[0]
            self.duplicates = self.df.duplicated()
        else:
            de_dup_size = self.df.drop_duplicates(cols).shape[0]
            self.duplicates = self.df[self.df.duplicated(subset=cols)]
        if de_dup_size != init_size:
            return True
        return False

    def get_duplicates(self, cols=[]):
        dups = self.check_duplicates(cols)
        if dups:
            return self.duplicates
        return False


def output_validation(outfile):
    # validator = OutputValidation(outfile)

    final_message = []
    # duplicates check
    # is_duplicate = validator.check_duplicates()
    # if is_duplicate:
    #     print 'Found duplicates in the output!'
    #     duplicates = validator.get_duplicates()
    #     final_message.append("Found duplicate rows {}!".format(duplicates.index.values))
    #
    # # # aspect wise duplicates identification
    # # as_duplicate = validator.check_duplicates(['source_review_id', 'aspect', 'sentiment_text'])
    # # if as_duplicate:
    # #     print 'Found aspect duplicates in some reviews!'
    # #     duplicates = validator.get_duplicates(['source_review_id', 'aspect', 'sentiment_text'])
    # #     final_message.append("Found duplicate reivew_id-aspect-sentiment_text combination {}!".format(
    # #         duplicates[['source_review_id', 'aspect', 'sentiment_text']].to_records(index=False)))
    #
    # # aspect wise duplicates identification
    # sent_duplicate = validator.check_duplicates(['source_review_id', 'aspect', 'sentiment_type'])
    # if sent_duplicate:
    #     print 'Found aspect duplicates in some reviews!'
    #     sent_duplicates = validator.get_duplicates(['source_review_id', 'aspect', 'sentiment_type'])
    #     print sent_duplicates[['source_review_id', 'aspect', 'sentiment_type']]
    #     final_message.append("Found duplicate reivew_id-aspect-sentiment combination \n{}!".format(
    #         sent_duplicates[['source_review_id', 'aspect', 'sentiment_type']].to_records(index=False)))

    if final_message:
        return {'errors': final_message, 'messages': None}
        # return {'errors':[]}
    else:
        return {'errors': None, 'messages': ['Successfully validated!']}


def file_validator(inputfile=None, outputfile=None, colmap={}):
    if not any([inputfile, outputfile]):
        return {'errors': ['No input provided! \nPlease provide input file name / output file name!']}

    if inputfile:
        try:
            raw_reviews = pd.read_csv(inputfile, sep=',', quotechar='"', escapechar='\\')
        except parsers.ParserError:
            try:
                raw_reviews = pd.read_csv(inputfile, sep=',', quoting=csv.QUOTE_ALL)
            except Exception as Ex:
                return {
                    "errors": ["There was some error reading the file! Please format the file with proper quoting and "
                               "delimiter as ','(comma)."],
                    "messages": ["There was some error reading the file! Please format the file with proper quoting "
                                 "and delimiter as ','(comma)."]}

        if raw_reviews.shape[0] == 0:
            return {"errors": [
                "The input file is having 0 rows!"],
                "messages": ["The input file is having 0 rows!"]}

        try:
            del raw_reviews['len']
            del raw_reviews['certifiedBuyer']
        except:
            pass
        rev_colmap = dict((y,x) for x, y in colmap.items())
        print rev_colmap
        raw_reviews.rename(columns=colmap ,inplace=True)
        # raw_reviews.rename(columns={'id': 'review_id', 'productId': 'product_id', 'rating': 'star_rating',
        #                             'title': 'review_title', 'text': 'review_text', 'createdOn': 'review_date'},
        #                    inplace=True)
        # Validate the input reviews
        validation_errors = inputfile_validation(raw_reviews, rev_colmap)

    elif outputfile:
        try:
            df = pd.read_csv(outputfile, sep='~', quoting=csv.QUOTE_ALL)
        except parsers.ParserError:
            df = pd.read_csv(outputfile, sep='~', quotechar='"', escapechar='\\')
        # Validate the output reviews
        validation_errors = output_validation(df)

    else:
        validation_errors = {'errors': [
            "In-sufficient number of arguments please provide input file / output file as input to the module."],
            'messages': None}

    return validation_errors


def unicode_str(text):
    try:
        text = text.decode('utf8')
    except:
        text = text
    return text


def order_columns(file_path, order, colmap):
    print file_path,"----->"
    try:
        df = pd.read_csv(file_path, sep=',', quotechar='"', escapechar='\\')
    except parsers.ParserError:
        try:
            df = pd.read_csv(file_path, sep=',', quoting=csv.QUOTE_ALL)
        except Exception as Ex:
            return {"errors": ["There was some error reading the file! Please format the file with proper quoting and "
                               "delimiter as ','(comma)."],
                    "messages": ["There was some error reading the file! Please format the file with proper quoting "
                                 "and delimiter as ','(comma)."]}

    # Converting file format for flipkart to format required for ml
    df.rename(columns=colmap ,inplace=True)
    df.rename(
        columns={'review_id': 'source_review_id', 'product_id': 'source_product_id', 'review_title': 'review_tag'},
        inplace=True)
    df = df.drop_duplicates(['source_product_id', 'source_review_id'])
    columns = list(df.columns.values)
    required_columns = ['source', 'product_name', 'review_date', 'star_rating',
                        'verified_user', 'reviewer_name', 'review_url', 'review_tag']
    for rcol in required_columns:
        if rcol not in columns:
            df[rcol] = ''

    df = df[order]
    df.review_text = df.review_text.apply(unicode_str)
    df.to_csv(file_path, sep='~', quoting=csv.QUOTE_ALL, index=False, encoding='utf-8')

    missing_cols = []
    for col in order:
        if col not in list(df.columns.values):
            missing_cols.append(col)
    if missing_cols:
        return {'messages': [], 'errors': ['{} columns are missing in the file!'.format(','.join(missing_cols))]}
    return {'messages': ['Validated and ordered the file!'], 'errors': []}


def check_file_header(filepath, columns, sep='~'):
    file_header = pd.read_csv(filepath, chunksize=2, sep=sep, quoting=csv.QUOTE_ALL).get_chunk()
    file_columns = file_header.columns.values
    missing_cols = []
    for col in columns:
        if col not in file_columns:
            missing_cols.append(col)
    return missing_cols


# Starting the module
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--inputfilename", type=str, required=False,
                        help='Input file uploaded for smartpulse generation.')
    parser.add_argument("-o", "--outputfilename", type=str, required=False,
                        help='output file generated by smartpulse script!')
    args = parser.parse_args()

    inputfile = args.inputfilename
    outputfile = args.outputfilename

    if not any([inputfile, outputfile]):
        raise ValueError(
            'No input provided! \nUsage: python file_validation_checks.py -i inputfile_path / -o outputfile_path')

    if inputfile:
        raw_reviews = pd.read_csv(inputfile, sep='~', escapechar='\\', quotechar='"')
        # Validate the input reviews
        print inputfile_validation(raw_reviews)

    elif outputfile:
        df = pd.read_csv(outputfile, sep='~', quotechar='"', escapechar='\\')
        # Validate the output reviews
        print output_validation(df)

    else:
        raise ValueError(
            "In-sufficient number of arguments please provide input file / output file as input to the module.")
