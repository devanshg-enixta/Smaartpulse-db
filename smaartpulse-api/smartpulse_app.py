from datetime import datetime
import time
from flask import Flask, session, make_response, request, flash, url_for, redirect, render_template, abort, g
import json
import requests
import random
import os, sys
from sqlalchemy import create_engine
import shutil
import zipfile
from smaartpulse import main as smartpulse
from db_methods import *
from file_validation_saas import file_validation_checks, duplicates_merge, google_nlp
import traceback
from utils import mailer
import yaml
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import logging
from logging.handlers import RotatingFileHandler
import cStringIO
import sqlalchemy
import config as app_config
from smaartpulse.prepare_metadata import LexiconMetadata
from utils.db_connection import get_engine

from multiprocessing import Process, Manager
import Queue

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

os.environ['S3_USE_SIGV4'] = 'True'

# logging.config.dictConfig(yaml.load(open('config/logging.conf')))
# logfile = logging.getLogger('file')
# logconsole = logging.getLogger('console')
# logfile.debug("Debug FILE")
# logconsole.debug("Debug CONSOLE")


app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
yaml_file = open("config/config.yaml", 'r')
config = yaml.load(yaml_file)
config['downloads_dir'] = os.path.join(BASE_DIR, config['downloads_dir'])
setattr(app_config, 'saas_config', config)
setattr(app_config, 'base_dir', BASE_DIR)
yaml_file.close()
alert_mails = config['alert_mails']
if isinstance(alert_mails, str):
    alert_mails = [alert_mails]


def send_mail(*args, **kwargs):
    email_type = config.get('email_type', None)
    email_config = config.get('MAIL_CONFIG')
    kwargs.update(email_config)
    if email_type:
        kwargs['type'] = config['email_type']
    mailer.send_mail(*args, **kwargs)


@app.route('/')
def index():
    response = make_response(
        json.dumps({"message": "Welcome to Enixta's smaartpulse module!", 'usage': "/validate-input?file=file_url"},
                   indent=4, encoding='utf-8'))
    response.headers['Content-Type'] = 'application/json'
    return response


def return_err_response(msg, code):
    app.logger.error("Sent Error Response as: " + str(msg))
    response = make_response(json.dumps(msg, indent=4, encoding='utf-8'))
    response.headers['Content-Type'] = 'application/json'
    response.status_code = code
    return response


def return_response(msg, code):
    app.logger.error("Sent Response as: " + str(msg))
    response = make_response(json.dumps(msg, indent=4, encoding='utf-8'))
    response.headers['Content-Type'] = 'application/json'
    response.status_code = code
    return response


def upload_to_s3(file_path, public=False):
    print '------public-------',public
    if not zipfile.is_zipfile(file_path):
        try:
            import zlib
            compression = zipfile.ZIP_DEFLATED
        except:
            compression = zipfile.ZIP_STORED
        zip_file_path = '.'.join(file_path.split('.')[:-1]) + '.zip'
        file_name = file_path.split('/')[-1]
        zf = zipfile.ZipFile(zip_file_path, mode='w')
        try:
            zf.write(file_path, file_name, compress_type=compression)
        finally:
            zf.close()
        try:
            os.remove(file_path)
        except:
            pass
    else:
        zip_file_path = file_path

    try:
        # os.environ['S3_USE_SIGV4'] = 'True'
        host = 's3.ap-south-1.amazonaws.com'
        c = S3Connection(config['AWS_ACCESS_KEY'], config['AWS_SECRET_KEY'], host=host)
        b = c.get_bucket(config['PROCESSED_FILE_DIR_S3'].split('/')[0])
        k = Key(b)
        k.key = os.path.join('/'.join(config['PROCESSED_FILE_DIR_S3'].split('/')[1:]), zip_file_path.split("/")[-1])
        f = open(zip_file_path, 'r+')
        file2 = cStringIO.StringIO()
        file2.write(f.read())
        k.set_contents_from_string(file2.getvalue())
        if bool(public):
            url = k.generate_url(expires_in=1 * 24 * 3600, query_auth=False)
        else:
            url = k.generate_url(expires_in=0, query_auth=False)
        c.close()

        # del os.environ['S3_USE_SIGV4']
        return url

    except Exception as Ex:
        trace = traceback.format_exc()
        app.logger.error(str(trace))
        #send_mail(alert_mails, "Some issue with the file upload to S3!", str(Ex))
        return False


def download_file(url):
    if os.path.exists(url):
        print"yes"
    else:
        print "no"
    file_name = url.split('/')[-1]
    extension = file_name.split('.')[-1]
    time_stamp = str(datetime.datetime.now())
    if not os.path.exists(config['downloads_dir']):
        os.makedirs(config['downloads_dir'])
    time_dir = os.path.join(config['downloads_dir'], time_stamp)
    if not os.path.exists(time_dir):
        os.makedirs(time_dir)
    output_file = config['downloads_dir'] + file_name

    try:
        host = 's3.ap-south-1.amazonaws.com'
        c = S3Connection(config['AWS_ACCESS_KEY'], config['AWS_SECRET_KEY'], host=host)

        file_path = url.split(host)[-1].strip('/')
        bucket_name = file_path.split('/')[0]
        file_path = '/'.join(file_path.split('/')[1:])
        print bucket_name
        b = c.get_bucket(bucket_name)  # substitute your bucket name here
        k = Key(b)
        k.key = file_path
        k.get_contents_to_filename(output_file)

        if extension == 'zip':
            zipper = zipfile.ZipFile(output_file)
            zipper.extractall(time_dir)
            os.remove(output_file)
            # try:
            #     shutil.rmtree(os.path.join(time_dir, '__MACOSX/'))
            # except:
            #     pass
            file_name = os.path.join(time_dir, [x for x in zipper.namelist() if x[0] != '_'][0])

        file_name2 = file_name.split('/')[-1].split('.')
        file_name2 = os.path.join(config['downloads_dir'],
                                  '.'.join(file_name2[:-1]) + '-' + str(datetime.datetime.now()) + '.' + file_name2[-1])
        shutil.move(file_name, file_name2)
        try:
            shutil.rmtree(time_dir)
        except:
            pass
        return file_name2
    except Exception as Ex:
        trace = traceback.format_exc()
        print file_path
        file_name2 = file_name.split('/')[-1].split('.')
        file_name2 = os.path.join(config['downloads_dir'],
                          '.'.join(file_name2[:-1]) + '-' + str(datetime.datetime.now()).replace(' ','') + '.' + file_name2[-1])
        shutil.move(url, file_name2)
        return file_name2
        # app.logger.error(str(trace))
        # #send_mail(alert_mails, "Some issue with the file download from S3!", str(Ex))
        # return return_err_response({
        #     'message': {"messages": ["There was some issue with file validation! An alert has sent to our internal "
        #                              "team. Soon we'll comeback to you!!"],
        #                 "errors": ["There was some issue with file validation! An alert has sent to our internal team. "
        #                            "Soon we'll comeback to you!!"]}}, 298)


@app.route('/validate-input')
def file_validation():
    args = dict(request.args)
    file_url = args.get("file", [None])[0]
    account_id = args.get("account_id", [1])[0]
    file_name = file_url.split('/')[-1]
    if file_name.split('.')[-1] not in ('csv', 'txt', 'zip'):
        return return_err_response({
            'message': {"messages": ["Invalid file type {}. Please give a file of the format .txt or "
                                     ".csv or .zip".format(
                file_name.split('.')[-1])],
                'errors': ['Invalid file format! Please upload a file of type .txt or .csv or .zip']}}, 299)

    if not file_url:
        response = make_response(
            json.dumps({"message": {"messages": ["Invalid input provided! Please provide proper file url."],
                                    "errors": ["Invalid input provided! Please give a proper file url."]}}, indent=4,
                       encoding='utf-8'))
        response.headers['Content-Type'] = 'application/json'
        response.status_code = 299
        return response
    else:
        file_name = download_file(file_url)
        if not isinstance(file_name, str):
            return file_name

        try:
            colmap = get_colmap(account_id)
            if not colmap:
                send_mail(alert_mails, "Error processing file {}! File configuration is missing "
                                       "for account id {}".format(file_name, account_id),
                          "Column mapping is missing for account id {}!".format(account_id))
                msg = {'message': {"errors": "File configuration is missing. We'll comeback to you soon!"}}
                return return_err_response(msg, 299)
            response = file_validation_checks.file_validator(inputfile=file_name, colmap=colmap)

            response = make_response(json.dumps({'message': response}, indent=4, encoding='utf-8'))
            response.headers['Content-Type'] = 'application/json'
            response.status_code = 200
            os.remove(file_name)
            return response

        except Exception as Ex:
            trace = traceback.format_exc()
            app.logger.error(str(trace))
            return return_err_response({'message': {"errors": ["Error in validating the file.\n{}".format(trace)]}},
                                       299)


# Upload the output snippets to db
def upload_output(pulse_config, file_id):
    result_dict = pulse_config.output

    # Read the treemap file from the result of smartpulse module
    treemap_file = result_dict['output_file']
    print "Upload output ", treemap_file
    # Verify the file structure and data whether it is as required
    checks = file_validation_checks.file_validator(outputfile=treemap_file)
    file_name = get_file_name(file_id)
    # If errors sendout a mail to the internal group
    if checks['errors']:
        # Output file validation error send mail to ml-team
        app.logger.error(str(checks['errors']))
        msg = {'message': checks}
        send_mail(alert_mails, "Error in generating smaartpulse for file {} (id {})".format(file_name, file_id),
               str('\n'.join(checks['errors'])).replace('\n', '<br/>'))
        return return_err_response(msg, 299)

    else:
        # if checks['errors'] and False:
        #     send_mail(['smaartalertinternal@enixta.com'],
        #                         "Error in generating smartpulse for file id {}".format(file_id),
        #                         str('\n'.join(checks['errors'])).replace('\n', '<br/>'))
        try:
            # Upload snippets to database
            upload = sentiment_upload(treemap_file, 'sentiment_output_temp', file_id)
            if upload:
                # Start validating the snippets

                # External lexicon validation implemented in backend
                lexicon_validation(file_id)

                # Update the not required snippets status to 'N'
                db = get_engine()
                db.execute(
                    "update sentiment_output_temp set validate_status='N', validate_comment='Aspect not required' "
                    "where aspect_id not in (select aspect_id from client_category_aspect "
                    "where (account_id, category_id) in (select c.account_id, f.category_id "
                    "from file_uploads f join customer c on c.id=f.customer_id where f.id={})) "
                    "and file_id={}".format(file_id, file_id))
                time.sleep(0.5)
                db.execute(
                    "update sentiment_output_temp set validate_status='N', validate_comment='Polarity not required' "
                    "where file_id={} "
                    "and LOWER(trim(sentiment_type)) not in ('positive','negative');".format(file_id))
                # Close the db connection
                db.dispose()

                app.index_merge_queue.append((file_id, 'index', 1))
                merged_files = app.index_merge_queue_output
                while file_id not in merged_files:
                    time.sleep(60)
                merged_files.remove(file_id)
                print "got merged file for nlp"
                gnlp = google_nlp_validation(file_id, file_name)
                if gnlp:
                    file_for_validation(file_id)
                else:
                    return return_err_response("Error in google nlp validation", 299)
            try:
                shutil.rmtree(pulse_config.client_dir)
            except:
                print "error in deleting client lexicon config dir"
            return return_response("successfully generated snippets.", 200)
        except Exception as Ex:
            trace = traceback.format_exc()
            app.logger.error(str(trace))
            msg = {'message': {'errors': [str(Ex)], 'error_stack': str(trace)}}
            send_mail(alert_mails,
                      "Error in uploading smartpulse to db for file {} with id {}".format(file_name, file_id),
                   str(Ex).replace('\\n', '<br/>')
                   + "<br/><br/><b>Error Stack:</b>" +
                   str(trace).replace('\\n', '<br/>'))
            return return_err_response(msg, 299)


# Extract the google nlp snippets for the reviews
def google_nlp_validation(file_id, file_name):
    try:
        google_nlp.google_nlp(file_id)
        return True
    except Exception as Ex2:
        trace2 = traceback.format_exc()
        send_mail(alert_mails,
                  "Error in extracting google nlp snippets for file {} with id {}.".format(file_name, file_id),
                  "Error: {}".format(str(trace2).replace('\\n', '<br/>')))
        return False


# Call the lexicon based output validator, implemented in (java backend)
def lexicon_validation(file_id):
    url = config['LEXICON_VALIDATOR']
    print url, file_id
    file_token = get_file_token(file_id)
    try:
        response = requests.get(url.format(file_token), timeout=15*60)
        print "file validator response ", response
    except Exception as Ex:
        print Ex


# function to generate file for validation after index and google nlp
def file_for_validation(file_id):
    file_name = get_file_name(file_id)

    # lexicon_nlp_validation(file_id, file_name)

    try:
        file_path, review_count, snippet_count, pmean, nmean, bias_msg = table_to_file('sentiment_output_temp',
                                                                                       file_id, sep=',')
        # url = '?'.join(upload_to_s3(file_path).split('?')[:-1])
        url = upload_to_s3(file_path, public=True)
    except Exception as s3Ex:
        url = False
        send_mail(alert_mails, "Error in extracting sentiment output to file and uploading to S3.",
                  str(s3Ex))
        return return_err_response('Failed uploading file to S3!', 299)
    msg = {'message': {'errors': [], 'messages': 'Successfully generated and uploaded the snippets!'},
           'url': url}

    if url:
        send_mail(alert_mails,
                  "Completed processing file {} with id {}!".format(file_name, file_id),
                  "Successfully completed smaartpulse generation and uploaded data to db! <br/>"
                  "Review Count: {}, <br/>Snippet Count: {}, <br/>Positive confidence Mean: {}, <br/>"
                  "<br/>Negative confidence Mean: {}, <br/>{}<br/>"
                  "You can download the file from the "
                  "following link. <br/> {}".format(review_count, snippet_count, pmean, nmean, bias_msg, url))
    else:
        send_mail(alert_mails,
                  "Completed processing file {} with id {}!".format(file_name, file_id),
                  "Successfully completed smaartpulse generation and uploaded data to db! <br/>"
                  "Review Count: {}, <br/>Snippet Count: {}, <br/>Positive confidence Mean: {}, <br/>"
                  "<br/>Negative confidence Mean: {}, <br/>{}<br/>Upload to S3 is failed. "
                  "Please check the logs for more details.<br/> {}".format(review_count, snippet_count, pmean,
                                                                           nmean, bias_msg, url))
    try:
        os.remove(file_path)
    except:
        pass
    return return_response(msg, 200)


# Send a parameter missing error response
def parameter_missing_error(parameters):
    msg = "Missing required parameter {}!".format(' '.join(parameters))
    msg = {'message': {'errors': [msg], 'messages': [msg]}}
    app.logger.error(msg)
    response = make_response(json.dumps(msg, indent=4, encoding='utf-8'))
    response.headers['Content-Type'] = 'application/json'
    response.status_code = 422
    return response


# End point to run the smartpulse module
@app.route('/run_smartpulse')
def run_smartpulse():
    args = dict(request.args)
    file_id = args.get('file_id', [None])[0]
    category_id = int(args.get('category_id', [0])[0])
    category = args.get('category', [None])[0]
    file_url = args.get('file', [None])[0]
    classification = "sentiment"
    account_id = int(args.get('account_id', [0])[0])

    if all([file_id, category_id, account_id, classification]):
        file_name_db = get_file_name(file_id)
        # download the file from the url
        file_name = download_file(file_url)
        colmap = get_colmap(account_id)
        if not colmap:
            send_mail(alert_mails, "Error processing file {}! File configuration is missing "
                                   "for account id {}".format(file_name_db, account_id),
                      "Column mapping is missing for account id {}!".format(account_id))
            msg = {'message': {"errors": "Column mapping is missing for account id {}! ".format(account_id)}}
            return return_err_response(msg, 299)

        # format the file as required for smartpulse module
        check_point = file_validation_checks.order_columns(file_name,
                                                           ['source', 'source_review_id', 'source_product_id',
                                                            'product_name', 'review_date', 'star_rating',
                                                            'verified_user',
                                                            'reviewer_name', 'review_url', 'review_tag', 'review_text'],
                                                           colmap)
        if check_point['errors']:
            app.logger.error(str(check_point['errors']))
            msg = {'message': check_point}
            return return_err_response(msg, 299)
        time.sleep(10)

        # Check the file_id existance in file_uploads
        file_exist = check_record('file_uploads', 'id', file_id)
        if not file_exist:
            send_mail(alert_mails,
                      "Error occured while uploading reviews to database for file id {}!".format(file_id),
                      "Invalid file id {}. File id is missing in the file_uploads table!".format(file_id))
            msg = {'message': {"errors": "No record for file_id {} in database! ".format(file_id)}}
            return return_err_response(msg, 299)

        # Todo: Create a function to extract the lexicon data from db and create config dir with that data :completed
        # Creating config for smartpulse generation
        root_dir = os.path.join(BASE_DIR, 'smaartpulse/lexicons_db')
        final_dir = os.path.join(BASE_DIR, 'downloads')
        db = get_engine()
        pulse_config = LexiconMetadata(root_dir, file_id, account_id, category_id, db, file_name, final_dir,
                                       classification)
        db.dispose()
        if not pulse_config.status:
            error_msg = pulse_config.status_msg
            send_mail(alert_mails,
                      "Lexicon config error for category {} with id {}.".format(pulse_config.category, category_id),
                      error_msg)
            return return_err_response({'errors': error_msg}, 299)

        # Start uploading the reviews to database
        try:
            review_uploader(pulse_config.input_file, 'source_reviews', file_id, category_id)
        except sqlalchemy.exc.IntegrityError as err:
            trace = traceback.format_exc()
            app.logger.error(str(trace))
            if "Duplicate entry" in str(err):
                msg = {'message': {"errors": "Some error has occured while uploading reviews to database! "
                                             "Duplicate entry for the table Source Reviews."}}
                send_mail(alert_mails,
                          "Error occured while uploading reviews to database for file {} with "
                          "id {}!".format(file_name_db, file_id),
                          "Duplicate entry for the table Source Reviews.")
                return return_err_response(msg, 299)
        except Exception as Ex:
            trace = traceback.format_exc()
            app.logger.error(str(trace))
            send_mail(alert_mails,
                      "Error occured while uploading reviews to database for file {} with "
                      "id {}!".format(file_name_db, file_id),
                      "There was error occured while uploading reviews!")
            msg = {'message': {"errors": "Some error has occured while uploading reviews to database! "
                                         "Sent an internal alert for notification."}}
            return return_err_response(msg, 299)

        # Trigger the smartpulse module
        smartpulse.trigger_smartpulse(pulse_config)
        status = pulse_config.output

        result = status.get('status', None)
        if result:
            return upload_output(pulse_config, file_id=file_id)

        else:
            app.logger.error(str(status['error_stack']))
            send_mail(alert_mails,
                      "Error in generating smartpulse for file {} with id {}".format(file_name, file_id),
                   str('\n'.join(status['errors'])).replace('\\n', '<br/>')
                   + "<br/><br/><b>Error Stack:</b>" +
                   str(status['error_stack']).replace('\\n', '<br/>'))
            return return_err_response(status, 299)
    else:
        missing_parameters = []
        for param, value in dict(zip(['file_id', 'category_id', 'accout_id', 'classification'],
                                     [file_id, category_id, account_id, classification])).items():
            if not value:
                missing_parameters.append(param)
        return parameter_missing_error(missing_parameters)


# End point to upload the output to db
@app.route('/upload_smartpulse')
def output_to_db():
    # Parsing parameters from the request
    args = dict(request.args)
    file_url = args.get('file', [None])[0]
    req_ip = request.remote_addr
    if file_url:
        # download the file from the url
        file_name = download_file(file_url)

        # Get column names of the target table from db
        table_cols = get_columns('sentiment_ouput')
        # Check the required columns present in the file or not
        missing_cols = file_validation_checks.check_file_header(file_name, table_cols, sep=',')
        if not missing_cols:
            # Uploading final files to DB
            file_creation = file_upload(file_name, 'sentiment_ouput', sep=',')

            # if errors in review send alert to internal mail group
            if file_creation['errors']:
                msg = {'message': file_creation}
                app.logger.error(str(file_creation['errors']))
                send_mail(alert_mails,
                          "Error in generating final output file for file {}".format(file_name),
                          "Error: {}".format(file_creation['errors']))
                return return_err_response(msg, 298)

            file_status = file_creation['data']
            file_id = file_status.get('id')
            file_token = file_status.get('token')

            # Start index generation and duplicate snippet merging
            duplicates_merge.duplicate_merge(file_id, 2)
            db = get_engine()

            # create the final output file for s3 upload
            file_path = create_final_output_file(file_id, db)
            if 'errors' in file_path:
                db.dispose()
                msg = {'message': {'errors':'Error in final file creation!'}}
                send_mail(alert_mails,
                          "Error in generating final output file for file {}".format(file_name),
                          "Error: {}".format(file_path['errors']))
                return return_err_response(msg, 298)
            db.dispose()

            file_path = file_path['file']

            # Upload the final output to s3
            final_file_url = upload_to_s3(file_path)

            # Update the s3 file url in db
            update_column(file_id=file_id, file_url=final_file_url)

            # Call the Complete analysis api to send an alert to the customer
            try:
                requests.get("https://{}:8443/completeAnalysis?fileToken={}".format(str(req_ip), file_token),
                             timeout=10, verify=False)
            except Exception as apiex:
                app.logger.error("Failed to call the process completion email api!\n{}".format(str(apiex)))

            return return_err_response({'message': file_creation},
                                       200)

        # For missing columns
        else:
            msg = {'message': {"errors": "Missing columns in the file!"
                                         "Few columns are missing in the file! {}".format(','.join(missing_cols))}}
            return return_err_response(msg, 298)

    # If file url is missing in the request parameters
    else:
        msg = {'message': {"errors": "Missing file_url in the parameter list!"}}
        return return_err_response(msg, 298)


# End point to download the output from db
@app.route('/download_sentiment')
def ouput_from_db():
    args = dict(request.args)
    file_id = args.get('file_id', [None])[0]
    if file_id:
        file_path = table_to_file('sentiment_output_temp', file_id, sep=',')
        url = '?'.join(upload_to_s3(file_path).split('?')[:-1])
        msg = {'message': {"messages": "Successfully Uploaded records to S3!"}, 'url': url}
        return return_response(msg, 200)
    else:
        msg = {'message': {"errors": "Missing file_id in the parameter list!"}}
        return return_err_response(msg, 298)


@app.after_request
def before_request(response):
    remote_addr = request.remote_addr
    if remote_addr == '::1':
        remote_addr = '127.0.0.1'
    app.logger.info("{} - [{} - {}] {}".format(remote_addr, request.method, response.status_code, request.full_path))
    return response


#######################################################################################################################
# Note: Planning a daemon process to create subprocesses for index generation
######
def daemon_process(input_queue, output_queue):
    while True:
        try:
            row = input_queue.pop(0)
            file_id, ftype, level = row
            print "got file {}".format(file_id)
            if ftype == 'index':
                duplicates_merge.duplicate_merge(file_id, level)
                output_queue.append(file_id)

        except IndexError:
            time.sleep(60)
            continue

        except KeyboardInterrupt:
            break

#######################################################################################################################


# Start the flask api server
if __name__ == '__main__':
    if not os.path.exists('logs/'):
        os.makedirs('logs/')

    # initialize the log handler
    logHandler = RotatingFileHandler('logs/saas.log', maxBytes=100000, backupCount=20)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)04d - %(message)s')
    logHandler.setFormatter(formatter)
    # set the log handler level
    logHandler.setLevel(logging.DEBUG)

    # app_log = logging.getLogger("tornado.application")
    # access_log = logging.getLogger("tornado.access")
    # gen_log = logging.getLogger("tornado.general")
    #
    # app_log.addHandler(logHandler)
    # access_log.addHandler(logHandler)
    # gen_log.addHandler(logHandler)

    app.logger_name = 'flask.app'
    app.logger.addHandler(logHandler)

    # set the app logger level
    app.logger.setLevel(logging.DEBUG)
    app.logger.addHandler(logHandler)

    # Starting a daemon for index merging and google nlp
    try:
        manager = Manager()
        input_queue = manager.list()
        output_queue = manager.list()
        # input_queue = list()
        daemon = Process(target=daemon_process, args=(input_queue,output_queue))
        daemon.start()
        setattr(app, 'index_merge_queue', input_queue)
        setattr(app, 'index_merge_queue_output', output_queue)

        app.run(threaded=True)
        # http_server = HTTPServer(WSGIContainer(app))
        # http_server.listen(9191)
        # IOLoop.instance().start()

    except KeyboardInterrupt:
        print "Got Keyboard Interrupt. Closing the Processes!"
        daemon.join()
        daemon.terminate()
        print "shutting down flask"