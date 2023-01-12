import requests
import boto3
import os
import tempfile
import datetime
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Ensure the S3 data in the local github data bucket is up-to-date
# Several steps need to be done:
#
# 1. Figure out the latest file in the github events bucket
# 2. Use yesterday as end date
# 3. Start the at latest file
# 4. For every hour, download file locally, upload to S3, delete


region_name = 'us-east-1'
s3 = boto3.client("s3", region_name=region_name)
bucket_name = 'sample-bucket-name'
bucket_prefix = 'sample-bucker-folder'

temp_dir = tempfile.TemporaryDirectory()

# Check S3 bucket for latest downloaded file
# Return the date + hour that requires download
def find_first_day_missing_uploads():
    date_start = datetime.date.today()

    while True:
       # check if first hour of day file exists
       file_name = '%04d-%02d-%02d-0.json.gz' % (date_start.year, date_start.month, date_start.day)
       s3_path = '%s/%04d/%02d/%02d/%s' % (bucket_prefix, date_start.year, date_start.month, date_start.day, file_name)
       is_file_missing = is_file_missing_in_s3(s3_path)
       print('Checked for file %s in S3. Missing: %s' % (s3_path, is_file_missing))
       if is_file_missing == False:
           break
       date_start = date_start - datetime.timedelta(days=1)

    # need to add one day again, as there are files on date_start
    return date_start + datetime.timedelta(days=1)

# Return true if the s3_path exists, false if not
# throws an exception in case of a different error than a 404
def is_file_missing_in_s3(path):
    try:
        s3.head_object(Bucket=bucket_name, Key=path)
        return False
    except s3.exceptions.ClientError as e:
        print(e)
        if e.response['Error']['Code'] == "404":
            return True
        else:
            raise e

# download file locally, upload to S3, delete local copy
def download_and_upload_file(year, month, day, hour):
    file_name = '%04d-%02d-%02d-%d.json.gz' % (year, month, day, hour)
    http_url = 'https://data.gharchive.org/%s' % (file_name)
    s3_path = '%s/%04d/%02d/%02d/%s' % (bucket_prefix, year, month, day, file_name)

    print('Downloading %s\n' % (http_url))
    response = requests.get(http_url)
    tmp_file_name = '%s/%s' % (temp_dir.name, file_name)
    with open(tmp_file_name, "wb") as f:
       f.write(response.content)

    print('Uploading %s to s3://%s/%s\n' % (tmp_file_name, bucket_name, s3_path))
    s3.upload_file(tmp_file_name, bucket_name, s3_path)

    print('Removing local copy %s\n' % (file_name))
    os.remove(tmp_file_name)

# from start onwards, till the yesterday, upload all files
def update_github_events_data():
    today = datetime.date.today()
    start = find_first_day_missing_uploads()

    if today == start:
        print('Only events from today missing, no need to update on partial days')
    else:
        # count every hour from start
        while start < today:
            for hour in range(0, 24):
                download_and_upload_file(start.year, start.month, start.day, hour)
            print('Finished updating GitHub events for %s\n' % (start))
            start = start + datetime.timedelta(days=1)

    # clean up temp resources
    temp_dir.cleanup()


# DAG default args
default_args = {
    'owner': '',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

# Create the DAG
dag = DAG(
    'check_github_events_dag',
    default_args=default_args,
    schedule_interval="0 8 * * *", # every morning, 8 am
    catchup=False
)

# Create a task using the PythonOperator to call the function
check_github_events_task = PythonOperator(
    task_id='check_github_events_task',
    python_callable=update_github_events_data,
    dag=dag
)

# Set the task dependencies
check_github_events_task
