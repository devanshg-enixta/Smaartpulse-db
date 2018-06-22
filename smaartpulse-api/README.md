# smaartpulse-api
Flask API service for Smaartpulse

# Validate input raw reviews file
http://(ip/url):9191/validate-input?file_id=143&account_id=1&category_id=1&file=https://s3.ap-south-1.amazonaws.com/enxsaastest1/ACN_extra_header2.txt%202_1515739410740.zip
# reply
{
    "message": {
        "errors": null,
        "messages": [
            "File upload is successful! Valid record count is 11."
        ]
    }
}


# Run Smaartpulse for the reviews validated
http://(ip/url):9191/run_smartpulse?file_id=143&account_id=1&category_id=1&file=https://s3.ap-south-1.amazonaws.com/enxsaastest1/ACN_extra_header2.txt%202_1515739410740.zip
# reply
{
    "message": {
        "errors": [],
        "messages": "Successfully generated and uploaded the snippets!"
    }
}


# Upload the validated output file to database
http://(ip/url):9191/upload_sentiment?file=https://s3.ap-south-1.amazonaws.com/enxsaastest1/ACN_extra_header2.txt%202_1515739410740.zip
# reply
{
    "message": {
        "errors": "",
        "messages": "Successfully uploaded the file to Sentiment Output! Valid record count is 1140."
    }
}


# download sentiment output for validation
http://(ip/url):9191/download_sentiment?file_id=143
# reply
{
    "message": {
        "messages": "Successfully Uploaded records to S3!"
    },
    "url": "https://enxsaastest1.s3.ap-south-1.amazonaws.com/downloads/sentiment_output_for_file_id_143_to_validate_2018-01-16%2012%3A51%3A47.485612.csv.zip"
}


# Setting up the project
1. Install Anacoda in the target machine
2. create a virtual environment with python 2.7.* (eg. $conda create -n env_name python=2.7.6)
3. Activate the environment ($source activate env_name)
4. Clone the project
5. Go to the project dir in shell
6. install the required packages using pip ($pip install -r requirements.txt)

   //Creating configurations required for the app
7. create directory named 'config' inside the project directory
    
    i. add a file named 'config.yaml' and add following contents
    
        AWS_ACCESS_KEY : ""
        AWS_SECRET_KEY : ""
        
        alert_mails:
                    "smaartalertinternal@enixta.com"
        
        contact_mail: "smaartalert@enixta.com"
        
        MAIL_CONFIG:
                    user: "GMAIL USERNAME"
                    passwd: "GMAIL PASSWORD/TOKEN"
        
        downloads_dir: "downloads/"
        
        PROCESSED_FILE_DIR_S3: "S3_BUCKET_PATH_FOR_PROCESSED_FILE"
        
        final_download_dir: "downloads/"
        
        email_type: "Test" # for testing environment
        
    ii. add a file named 'databases.yaml' and add the following contents
    
        saas:
          host: "localhost"
          port: 3306
          user: "root"
          passwd: "password"
          db: "database"
          
8. start the app ($python smartpulse_app.py)