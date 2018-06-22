import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import yaml
from utils import mailer
import logging
# from smartpulse_app import app
import config as app
from pandas.io import parsers
import datetime, time
import zipfile
import os
import csv
from utils.db_connection import get_engine


def get_colmap(account_id):
    con = get_engine()
    col_maps = con.execute("select source_column_name, target_column_name from source_review_column_map "
                           "where account_id={};".format(account_id))
    col_maps = dict([(x[0].strip(), x[1].strip()) for x in col_maps.fetchall()])
    con.dispose()
    return col_maps


def get_columns(table):
    con = get_engine()
    cols = con.execute("desc {};".format(table))
    cols = [x[0] for x in list(cols) if x and x[0] not in [u'id', u'created_date', u'modified_date']]
    con.dispose()
    return cols


def check_record(table, column, value):
    con = get_engine()
    recs = 0
    if str(value).isdigit():
        recs = con.execute("select * from {} where {}={};".format(table, column, value))
    else:
        recs = con.execute("select * from {} where {}='{}';".format(table, column, value))
    con.dispose()
    if len(list(recs)):
        return True
    else:
        return False


# Get file_name from file_uploads table based on file_id
def get_file_name(file_id):
    is_id = check_record('file_uploads', 'id', file_id)
    if not is_id:
        return False
    con = get_engine()
    file_name = con.execute("select file_name from file_uploads where id={};".format(file_id))
    file_name = list(file_name.fetchone())[0]
    return file_name


# Get file_name from file_uploads table based on file_id
def get_file_token(file_id):
    is_id = check_record('file_uploads', 'id', file_id)
    if not is_id:
        return False
    con = get_engine()
    file_token = con.execute("select file_token from file_uploads where id={};".format(file_id))
    file_token = list(file_token.fetchone())[0]
    return file_token


def review_uploader(file_name, table, file_id, category_id):
    con = get_engine()
    data = pd.read_csv(file_name, sep='~', quoting=csv.QUOTE_ALL, encoding='utf-8')
    data['file_id'] = int(file_id)
    data.rename(columns={'source_review_id': 'review_id', 'source_product_id': 'product_id',
                         'review_tag': 'review_title'}, inplace=True)
    data['category_id'] = category_id
    data = data[
        ['product_id', 'review_id', 'product_name', 'reviewer_name', 'star_rating', 'review_date', 'review_title',
         'category_id', 'review_url', 'review_text', 'file_id']]
    data.to_sql(table, if_exists='append', con=con, index=False, chunksize=1000)
    con.execute("update file_uploads set reviews_uploaded = (select count(*) from source_reviews "
                "where file_id=file_uploads.id);")
    con.execute("update account_subscription set reviews_uploaded = (select count(*) from source_reviews sr "
                "join file_uploads fp on fp.id=sr.file_id join customer cr on cr.id=fp.customer_id "
                "where cr.account_id=account_subscription.account_id), "
                "category_count=(select count(distinct sr.category_id) from source_reviews sr "
                "join file_uploads fp on fp.id=sr.file_id join customer cr on cr.id=fp.customer_id "
                "where cr.account_id=account_subscription.account_id);")
    # Close the connection
    con.dispose()


def checks_for_final_output(df):
    # 1. Column names check
    df_columns = df.columns.values
    req_columns = ['product_id', 'review_id', 'aspect_name', 'sentiment_text', 'sentiment_type', 'confidence_score',
                   'start_index_sentiment', 'end_index_sentiment', 'start_index_partial_review',
                   'end_index_partial_review', 'partial_review_text', 'full_review_text']
    missing_cols = []
    for col in req_columns:
        if not col in df_columns:
            missing_cols.append(col)
    if missing_cols:
        return df, 'Missing required columns {}.'.format(','.join(missing_cols))

    # 2. Start index and end-index check
    null_start_end_index = df[df.start_index_sentiment.isnull() | df.end_index_sentiment.isnull()]
    if not null_start_end_index.empty:
        return df, "Missing start_index/ end_index in the final file"

    # 8. Start index > end-index check
    null_start_end_index = df[(df.start_index_sentiment > df.end_index_sentiment)]
    if not null_start_end_index.empty:
        return df, "found rows with start_index > (greater than) end_index"

    # 3. Sentiment text, filter nulls
    sentiment_text_null = df[df.sentiment_text.isnull()]
    if not sentiment_text_null.empty:
        return df, "Null sentiment text values found"

    # 4. Full review text, filter nulls
    review_text_null = df[df.full_review_text.isnull()]
    if not review_text_null.empty:
        return df, "Null review text values found"

    # 5. partial review indexes and text, filter nulls
    partial_review_index_null = df[df.start_index_partial_review.isnull() | df.end_index_partial_review.isnull()]
    if not partial_review_index_null.empty:
        return df, "Missing partial review text indexes in the file!"

    partial_review_text_null = df[df.partial_review_text.isnull()]
    if not partial_review_text_null.empty:
        return df, "Missing partial review text in the file!"

    positive = df[df.sentiment_type == 'positive'].confidence_score
    negative = df[df.sentiment_type == 'negative'].confidence_score
    pmean, pstd = positive.mean(), positive.std()
    nmean, nstd = negative.mean(), negative.std()
    norm = abs(pmean-nmean)/(pmean+nmean)
    if norm > 0.2:
        return df, "Positive and negative confidence_score distributions are non-matching.<br/>Positive Mean: {}, " \
                   "<br/>Negative Mean: {}".format(pmean, nmean)
    if pmean < 0.5:
        return df, "Positive confidence_score distribution is biased towards zero."
    if nmean < 0.5:
        return df, "Negative confidence_score distribution is biased towards zero."

    # 6. drop unwanted polarity
    df = df[~(df.sentiment_type.isnull())]
    df.sentiment_type = df.sentiment_type.str.lower()
    df = df[df.sentiment_type.isin(['positive','negative'])]

    # 7. drop single word sentiment text
    df = df[~(df.sentiment_text.isnull())]
    df.sentiment_text = df.sentiment_text.apply(lambda x: x if len(x.split())>1 else pd.np.NaN)
    df = df[~(df.sentiment_text.isnull())]
    return df, None


def update_snippet_counts(file_id, count):
    con = get_engine()
    con.execute(text("update file_uploads set reviews_uploaded = (select count(*) from source_reviews "
                     "where file_id=file_uploads.id), "
                     "review_fragments_generated = {};".format(count)).execution_options(autocommit=True))

    con.execute(text("update account_subscription set reviews_uploaded = (select count(*) from source_reviews sr "
                     "join file_uploads fp on fp.id=sr.file_id "
                     "join customer cr on cr.id=fp.customer_id where cr.account_id=account_subscription.account_id), "
                     "total_fragment_generated = (select sum(review_fragments_generated) from  file_uploads fp "
                     "join customer cr on cr.id=fp.customer_id "
                     "where cr.account_id=account_subscription.account_id), "
                     "category_count=(select count(distinct sr.category_id) from source_reviews sr "
                     "join file_uploads fp on fp.id=sr.file_id join customer cr on cr.id=fp.customer_id "
                     "where cr.account_id=account_subscription.account_id);").execution_options(autocommit=True))

    # con.execute("update file_uploads set status='3' where id in ({});".format(''.join(file_ids)))

    con.dispose()


def create_final_output_file(file_id, con):
    table_recs = pd.read_sql("select sot.*, sr.review_text from sentiment_ouput sot join (select file_id, review_id, "
                             "review_text from source_reviews where file_id={}) sr on sr.review_id=sot.review_id "
                             "where sot.file_id={};".format(file_id, file_id), con=con)
    category_name = con.execute("select c.category_name, f.file_name from category c join file_uploads f "
                                "on f.category_id=c.id where f.id={};".format(file_id))
    category_name, file_name = category_name.fetchone()
    table_recs = table_recs.to_dict('records')
    for row in table_recs:
        start_index = row['start_index']
        end_index = row['end_index']
        try:
            review_text = row['review_text'].decode('utf8')
        except:
            review_text = row['review_text']

        # Todo: Move the partial review text length to config
        expected_start_index = start_index - int(app.saas_config['PARTIAL_REVIEW_LENGTH'])
        expected_end_index = end_index + int(app.saas_config['PARTIAL_REVIEW_LENGTH'])
        if expected_start_index < 0:
            expected_start_index = 0
        else:
            while (expected_start_index - 1) >= 0 and review_text[expected_start_index - 1] != ' ':
                expected_start_index -= 1
        if expected_end_index > len(review_text):
            expected_end_index = len(review_text)
        else:
            while (expected_end_index + 1) < len(review_text) and review_text[expected_end_index] != ' ':
                expected_end_index += 1

        row['start_index_partial_review'], row['end_index_partial_review'] = expected_start_index, expected_end_index
        row['partial_review_text'] = review_text[expected_start_index:expected_end_index].strip('"').strip("'").strip()

    # Rename the column names according to the client requirement
    table_recs = pd.DataFrame(table_recs).rename(columns={'aspect_id': 'aspect_name',
                                                          'start_index': 'start_index_sentiment',
                                                          'end_index': 'end_index_sentiment',
                                                          'review_text': 'full_review_text'})

    outfile_path1 = app.saas_config['downloads_dir'] + "{}_{}_Validation_Product_User_Review_Sentiment.csv" \
        .format(get_datestr(), category_name)

    table_recs = table_recs[['product_id', 'review_id', 'aspect_name', 'sentiment_text', 'sentiment_type',
                             'confidence_score', 'start_index_sentiment', 'end_index_sentiment',
                             'start_index_partial_review', 'end_index_partial_review', 'partial_review_text',
                             'full_review_text']]
    # Verify the final file
    table_recs, error_msg = checks_for_final_output(table_recs)
    if error_msg:
        return {'errors':error_msg}

    update_snippet_counts(file_id, table_recs.shape[0])

    table_recs.to_csv(outfile_path1, index=False, quoting=csv.QUOTE_ALL, encoding='utf8')
    outfile_path2 = app.saas_config['downloads_dir'] + "{}_{}_DB_Ingestion_Product_User_Review_Sentiment.csv" \
        .format(get_datestr(), category_name)
    del table_recs['sentiment_text']
    del table_recs['partial_review_text']
    del table_recs['full_review_text']
    table_recs.to_csv(outfile_path2, index=False, quoting=csv.QUOTE_ALL, encoding='utf8')

    try:
        import zlib
        compression = zipfile.ZIP_DEFLATED
    except:
        compression = zipfile.ZIP_STORED
    zip_file_path = app.saas_config['final_download_dir'] + file_name + '_' + str(int(file_id)) + 'final_output.zip'

    file_name1 = os.path.basename(outfile_path1)
    file_name2 = os.path.basename(outfile_path2)
    print file_name1, file_name2
    zf = zipfile.ZipFile(zip_file_path, mode='w')
    try:
        zf.write(outfile_path1, file_name1, compress_type=compression)
        zf.write(outfile_path2, file_name2, compress_type=compression)
    finally:
        zf.close()
    try:
        os.remove(outfile_path1)
        os.remove(outfile_path2)
    except:
        pass
    return {'file': zip_file_path}


def check_aspects(file_id, file_aspects, con):
    aspects_query = "select aspect_name from client_category_aspect where (account_id, category_id) " \
                    "in (select c.account_id, f.category_id from file_uploads f " \
                    "join customer c on c.id=f.customer_id " \
                    "where f.id={});".format(file_id)
    aspects = list(pd.read_sql(aspects_query, con=con).aspect_name.unique())
    print aspects, file_aspects
    extra_aspects = []
    for asp in file_aspects:
        if asp not in aspects:
            extra_aspects.append(asp)
    return extra_aspects


def file_upload(file_path, table, sep=',', req_ip="kiwi.enixta.com"):
    con = get_engine()
    try:
        records = pd.read_csv(file_path, sep=sep, quoting=csv.QUOTE_ALL)
    except parsers.ParserError:
        try:
            records = pd.read_csv(file_path, sep=sep, quotechar='"', escapechar='\\')
        except Exception as Ex:
            con.dispose()
            return {
                "errors": ["There was some error reading the file! Please format the file with proper quoting and "
                           "delimiter as ','(comma)."],
                "messages": ["There was some error reading the file! Please format the file with proper quoting "
                             "and delimiter as ','(comma)."]}
    records = records[~(records.file_id.isnull())]
    file_ids = map(int, map(float, map(str, list(records.file_id.unique()))))

    # Drop the unwanted columns
    records = records[['category_id','file_id','review_id','product_id','aspect_name','sentiment_type','sentiment_text',
                       'start_index','end_index','confidence_score']]

    # Strip the starting and trailing spaces for text fields
    records = records.apply(lambda row: [x.strip() if isinstance(x, (str, unicode)) else x for x in row], axis=1)

    # Check the existence of file_id in db
    missing_file_ids = []
    for file_id in file_ids:
        is_file_id = check_record('file_uploads', 'id', file_id)
        if not is_file_id:
            missing_file_ids.append(file_id)
    if missing_file_ids:
        con.dispose()
        return {
            "errors": "Missing file_ids in database {}! ".format(','.join(map(str, missing_file_ids))),
            "messages": ""}

    # check for multiple file ids in the file
    if len(file_ids)>1:
        con.dispose()
        return {
            "errors": "Multiple file ids found in the file {}.".format(file_path),
            "messages": ""}

    # Raise error if there are no file ids present
    if len(file_ids)==0:
        con.dispose()
        return {
            "errors": "File id column is empty in the file {}.".format(file_path),
            "messages": ""}

    # get the unique aspect names from the file
    aspect_names = list(records.aspect_name.unique())
    # check for the missing / extra aspects present in the file
    extra_aspects = check_aspects(file_ids[0], aspect_names, con)
    if extra_aspects:
        # if extra aspects present in the file return an error
        con.dispose()
        return {
            "errors": "Not required aspect names are present in the file!\n{}".format(','.join(map(str,
                                                                                                   extra_aspects))),
            "messages": ""}

    # Todo: Need to add the error return code to polarity also
    # remove the not required polarity snippets in the file
    records = records[records.sentiment_type.isin(['positive','negative'])]

    # Delete the entries from the db table if pre-existing
    if file_ids:
        # Todo: Try to suppress the reference check in db
        con.execute("delete from {} where file_id in ({});".format(table, ','.join(map(str, file_ids))))
    # Todo: make db upload async
    records.to_sql(table, if_exists='append', con=con, index=False, chunksize=1000)

    file_id = file_ids[0]
    user_mail = con.execute("select email, first_name, file_token from customer c join file_uploads f on "
                            "f.customer_id=c.id where f.id={};".format(file_id))
    user_mail, user_name, file_token = user_mail.fetchone()
    final_files = {'id':file_id, 'token': file_token}

    con.dispose()
    return {
        "data":final_files,
        "errors": "",
        "messages": "Successfully uploaded the file to {}! "
                    "Valid record count is {}.".format(table.replace('_', ' ').title(), records.shape[0])}


def update_column(file_id, file_url):
    con = get_engine()
    try:
        file_url = file_url.split("?")[0]
        query = """update file_uploads set download_url="{}" where id={};""".format(file_url, file_id)
        con.execute(query)
        time.sleep(1)
        con.dispose()
        return True
    except Exception as Ex:
        con.dispose()
        logging.error("Failed to update download url!\n{}".format(str(Ex)))
        return False


def get_datestr():
    return datetime.datetime.today().strftime('%Y-%m-%d')


def get_timestamp():
    return str(datetime.datetime.now())


def zip_files(files, zip_file_path):
    try:
        import zlib
        compression = zipfile.ZIP_DEFLATED
    except:
        compression = zipfile.ZIP_STORED

    zf = zipfile.ZipFile(zip_file_path, mode='w')

    for filename in files:
        base_name = os.path.basename(filename)
        try:
            zf.write(filename, base_name, compress_type=compression)
        except Exception as exp:
            return {'errors':"Error in zipping the file. {}".format(exp)}
    zf.close()
    return {"errors":False}


def table_to_file(table, file_id, sep=','):
    con = get_engine()
    table_recs = pd.read_sql("select o.*, sr.review_text from {} o join source_reviews sr on sr.file_id=o.file_id "
                             "and sr.review_id=o.review_id where o.file_id={} "
                             "and sr.file_id={};".format(table, file_id, file_id), con=con)
    reviews = pd.read_sql("select review_id from source_reviews where file_id={};".format(file_id), con=con)

    aspect_names = pd.read_sql("select aspect_id, aspect_name "
                               "from client_category_aspect "
                               "where (account_id, category_id) in (select c.account_id, f.category_id "
                               "from file_uploads f join customer c on c.id=f.customer_id "
                               "where f.id={});".format(file_id), con=con)

    cat_aspect_names = pd.read_sql("select id as aspect_id, aspect_name from category_aspect where category_id in "
                                   "(select category_id from file_uploads where id={});".format(file_id), con=con)

    # Coercing to int because the db column of aspect_id is varchar as we were using aspect name
    table_recs.aspect_id = table_recs.aspect_id.map(int)

    # separate the validate status 'Y' and 'N'
    error_log = table_recs[~(table_recs.validate_status == 'Y')]
    table_recs = table_recs[(table_recs.validate_status == 'Y')]

    # Merge the aspect names based on aspect_id
    table_recs = table_recs.merge(aspect_names, on=['aspect_id'], how='left')
    error_log = error_log.merge(cat_aspect_names, on=['aspect_id'], how='left')

    # Extracting counts
    snippet_count = table_recs.shape[0]
    review_count = reviews.shape[0]

    positive = table_recs[table_recs.sentiment_type == 'positive'].confidence_score
    negative = table_recs[table_recs.sentiment_type == 'negative'].confidence_score
    pmean, pstd = positive.mean(), positive.std()
    nmean, nstd = negative.mean(), negative.std()
    norm = abs(pmean - nmean) / (pmean + nmean)
    if norm > 0.2:
        bias_msg = "Positive and negative confidence_score distributions are non-matching."
    elif pmean < 0.5:
        bias_msg = "Positive confidence_score distribution is biased towards zero."
    elif nmean < 0.5:
        bias_msg = "Negative confidence_score distribution is biased towards zero."
    else:
        bias_msg = "Confidence Score distribution seems good."

    del table_recs['aspect_id']
    del table_recs['id']
    try:
        del table_recs['validate_status']
        del table_recs['validate_comment']
    except:
        pass

    category_name = con.execute("select c.category_name, f.file_name from category c join file_uploads f "
                                "on f.category_id=c.id where f.id={};".format(file_id))
    category_name, raw_file_name = category_name.fetchone()
    con.dispose()

    base_name = "{}_{}_output.csv".format(raw_file_name, file_id)
    base_name2 = "{}_{}_error_log.csv".format(raw_file_name, file_id)
    file_name = app.saas_config['downloads_dir'] + base_name
    file_name2 = app.saas_config['downloads_dir'] + base_name2
    table_recs.to_csv(file_name, sep=sep, quoting=csv.QUOTE_ALL, encoding='utf8', index=False)
    error_log.to_csv(file_name2, sep=sep, quoting=csv.QUOTE_ALL, encoding='utf8', index=False)
    zip_file_path = app.saas_config['downloads_dir'] + base_name.replace('.csv','.zip')
    zip_status = zip_files([file_name, file_name2], zip_file_path)
    if zip_status['errors']:
        raise(ValueError, "Failed in zipping the files!")
    try:
        os.remove(file_name)
        os.remove(file_name2)
    except:
        pass
    return zip_file_path, review_count, snippet_count, pmean, nmean, bias_msg


def sentiment_upload(file_name, table, file_id):
    con = get_engine()
    data = pd.read_csv(file_name, sep='~', quoting=csv.QUOTE_ALL, encoding='utf-8')
    data['file_id'] = int(file_id)
    data = data[['category_id', 'file_id', 'source_review_id', 'source_product_id', 'aspect', 'sentiment_type',
                 'sentiment_text', 'start_index', 'end_index', 'confidence-score']]
    data.rename(columns={'source_review_id': 'review_id', 'source_product_id': 'product_id', 'aspect': 'aspect_id',
                         'confidence-score': 'confidence_score'}, inplace=True)
    con.execute("delete from {} where file_id in ({});".format(table, file_id))
    print "Upload is started"

    data = data.sort_values(['confidence_score'], ascending=False)
    data = data.drop_duplicates(['product_id', 'review_id', 'aspect_id', 'sentiment_text'], keep='first')
    data.to_sql(table, if_exists='append', con=con, index=False, chunksize=1000)
    # con.execute("update file_uploads set status='3' where id={};".format(file_id))
    print "Upload is completed for file {}".format(file_id)
    con.execute("update file_uploads set status=2 where id = {};".format(file_id))
    # Send notification mail to the user
    # user_mail = con.execute("select email, first_name, file_token from customer c join file_uploads f on "
    # "f.customer_id=c.id where f.id={};".format(file_id))
    # user_mail, user_name, file_token = user_mail.fetchone()

    # Close the connection
    con.dispose()
    # try:
    #     requests.get("http://192.168.2.112:8080/completeAnalysis?fileToken={}".format(file_token), timeout=5)
    #     # send_mail.send_mail('smaartalert@enixta.com', [user_mail], "Completed processing your file!", "Dear {},
    #     # <br/> Successfully processed your reviews file and extracted the sentiment snippets." "<br/>You can
    #     # download the file from the downloads section!".format(user_name))
    # except:
    #     pass
    return True
