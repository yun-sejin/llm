import boto3
import zipfile
import os
import glob

# Please replace 'your_access_key', 'your_secret_key', 'your_bucket_name', 'c:/workplace/airflow/llm', and 'c:/workplace/airflow/llm/extract/files' with your actual AWS access key, secret key, bucket name, the local path where you want to download the zip files, and the path where you want to extract the zip files.

# This code will delete all files in the specified local path and extract path, download all the zip files from the specified S3 bucket to the local path, and then extract each zip file to the extract path. The download_file_from_s3 function downloads a file from S3 to the local path. The unzip_file function opens a zip file and extracts all its contents to the extract path. The delete_files_in_directory function deletes all files in the specified directory. The download_and_unzip_files function first calls delete_files_in_directory to delete all files in the local path and extract path, then it gets a list of all the files in the S3 bucket, loops over the list, calls download_file_from_s3 to download each file, and then calls unzip_file to extract each file.
#from airflow.models import Variable
import yaml

from llm.qna_insert import read_json_file, upsert_data
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# with open(r'C:\workplace\new\llm\config.yaml', 'r') as f:
#     config = yaml.safe_load(f)

# aws_access_key_id = config['aws']['access_key_id']
# aws_secret_access_key = config['aws']['secret_access_key']
# bucket_name = config['aws']['bucket_name']
# local_path = config['paths']['local_path']
# extract_path = config['paths']['extract_path']

# aws_access_key_id = Variable.get('aws_access_key_id')
# aws_secret_access_key = Variable.get('aws_secret_access_key')
# bucket_name = Variable.get('bucket_name')


# aws_conn_id는 Apache Airflow에서 AWS 서비스에 연결하기 위해 사용하는 연결 ID입니다. 이는 Airflow의 연결 관리 시스템에 설정된 특정 AWS 연결의 식별자로, AWS 자격 증명(예: 액세스 키 ID, 비밀 액세스 키) 및 기타 연결 설정(예: 리전)을 포함합니다. aws_conn_id를 사용하면 DAG(Directed Acyclic Graph) 코드 내에서 직접 자격 증명을 관리할 필요 없이, Airflow UI를 통해 미리 설정된 연결 정보를 재사용할 수 있습니다.

# 기본적으로, 많은 Airflow AWS 관련 연산자와 훅은 aws_conn_id의 기본값으로 'aws_default'를 사용합니다. 하지만, 사용자는 Airflow UI의 "Connections" 섹션에서 새 연결을 추가하고, 이 연결에 대한 고유한 ID를 지정하여 여러 AWS 계정이나 다양한 설정을 사용할 수 있습니다. 이렇게 하면 동일한 Airflow 인스턴스에서 여러 AWS 환경에 대한 작업을 유연하게 처리할 수 있습니다.

def download_file_from_s3(bucket_name, s3_file_name, local_file_name):
    hook = S3Hook(aws_conn_id='aws_default')
    hook.download_file(bucket_name=bucket_name, key=s3_file_name, local_path=local_file_name)

def unzip_file(local_zip_name, extract_path):
    with zipfile.ZipFile(local_zip_name, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

def delete_files_in_directory(directory):
    files = glob.glob(f'{directory}/*')
    for f in files:
        os.remove(f)

def download_and_unzip_files(bucket_name, local_path, extract_path):
    hook = S3Hook(aws_conn_id='aws_default')
    keys = hook.list_keys(bucket_name=bucket_name)
    for s3_file_name in keys:
        if s3_file_name.endswith('.zip'):  # Only process .zip files
            local_zip_name = f"{local_path}/{s3_file_name}"
            download_file_from_s3(bucket_name, s3_file_name, local_zip_name)
            unzip_file(local_zip_name, extract_path)
           

# Usage
download_and_unzip_files('your_bucket_name', 'c:/workplace/airflow/llm', 'c:/workplace/airflow/llm/extract/files')