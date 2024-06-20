import boto3
import zipfile
import os
import glob

# Please replace 'your_access_key', 'your_secret_key', 'your_bucket_name', 'c:/workplace/airflow/llm', and 'c:/workplace/airflow/llm/extract/files' with your actual AWS access key, secret key, bucket name, the local path where you want to download the zip files, and the path where you want to extract the zip files.

# This code will delete all files in the specified local path and extract path, download all the zip files from the specified S3 bucket to the local path, and then extract each zip file to the extract path. The download_file_from_s3 function downloads a file from S3 to the local path. The unzip_file function opens a zip file and extracts all its contents to the extract path. The delete_files_in_directory function deletes all files in the specified directory. The download_and_unzip_files function first calls delete_files_in_directory to delete all files in the local path and extract path, then it gets a list of all the files in the S3 bucket, loops over the list, calls download_file_from_s3 to download each file, and then calls unzip_file to extract each file.

s3 = boto3.client('s3', aws_access_key_id='your_access_key', aws_secret_access_key='your_secret_key')

def download_file_from_s3(bucket_name, s3_file_name, local_file_name):
    s3.download_file(bucket_name, s3_file_name, local_file_name)

def unzip_file(local_zip_name, extract_path):
    with zipfile.ZipFile(local_zip_name, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

def delete_files_in_directory(directory):
    files = glob.glob(f'{directory}/*')
    for f in files:
        os.remove(f)

def download_and_unzip_files(bucket_name, local_path, extract_path):
    delete_files_in_directory(local_path)  # Delete all files in the local path
    delete_files_in_directory(extract_path)  # Delete all files in the extract path

    response = s3.list_objects_v2(Bucket=bucket_name)
    for object in response['Contents']:
        s3_file_name = object['Key']
        if s3_file_name.endswith('.zip'):  # Only process .zip files
            local_zip_name = f"{local_path}/{s3_file_name}"
            download_file_from_s3(bucket_name, s3_file_name, local_zip_name)
            unzip_file(local_zip_name, extract_path)

# Usage
download_and_unzip_files('your_bucket_name', 'c:/workplace/airflow/llm', 'c:/workplace/airflow/llm/extract/files')