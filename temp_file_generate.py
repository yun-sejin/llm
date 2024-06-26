import os
import zipfile
import shutil

# Variable for the folder name
folder_name = "aaa"

# Base directory where the folder is located
base_dir = "C:/workplace/airflow/llm/file"

# Full path to the target folder
target_folder = os.path.join(base_dir, folder_name)

# Iterate through each file in the target folder
for file in os.listdir(target_folder):
    if file.endswith(".zip"):
        # Full path to the zip file
        zip_file_path = os.path.join(target_folder, file)
        
        # Unzip the file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(target_folder)

        print(f"Extracted {file} in {target_folder}")


##################################################
        ##대상 폴더의 하위 전체 파일을 순회하도록 변경
###################################################

# Variable for the folder name
folder_name = "aaa"

# Base directory where the folder is located
base_dir = "C:/workplace/airflow/llm/file"

# Full path to the target folder
target_folder = os.path.join(base_dir, folder_name)

# Iterate through each file in the target folder and its subfolders
for root, dirs, files in os.walk(target_folder):
    for file in files:
        if file.endswith(".zip"):
            # Full path to the zip file
            zip_file_path = os.path.join(root, file)
            
            # Unzip the file
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(root)

            print(f"Extracted {file} in {root}")  

##################################################
        ##file명이 list = ['aaa.zip','bbb.zip','ccc.zip']으로 선언된 값중에 포함되는지로 변환
###################################################      

# Variable for the folder name
folder_name = "aaa"

# Base directory where the folder is located
base_dir = "C:/workplace/airflow/llm/file"

# Full path to the target folder
target_folder = os.path.join(base_dir, folder_name)

# List of specific file names to look for
file_names = ['aaa.zip', 'bbb.zip', 'ccc.zip']

# Iterate through each file in the target folder and its subfolders
for root, dirs, files in os.walk(target_folder):
    for file in files:
        if file in file_names:
            # Full path to the file
            file_path = os.path.join(root, file)
            
            # Unzip the file
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(root)

            print(f"Extracted {file} in {root}")


    ##########################
    #1. s3파일 다운로드 전 로컬 파일 삭제
    #2. s3전체 파일 다운로드
    ##########################
#1.s3파일 다운로드 전 로컬 파일 삭제
    # Path to the folder you want to delete
folder_path = "C:/workplace/airflow/llm/file"

# Delete the folder and all its contents
shutil.rmtree(folder_path)

print(f"Deleted folder: {folder_path}")

#2.s3전체 파일 다운로드
# AWS S3 setup
s3 = boto3.client('s3')
bucket_name = 'your-bucket-name'  # Replace with your bucket name

# The directory where you want to download the files
download_directory = "C:/workplace/airflow/llm/file"

# Ensure the download directory exists
os.makedirs(download_directory, exist_ok=True)

# os.makedirs(download_directory, exist_ok=True) 코드는 Python에서 사용되며, 다음과 같은 작업을 수행합니다:

# download_directory 변수에 지정된 경로에 디렉토리(폴더)를 생성합니다. 이 경로는 중첩된 디렉토리를 포함할 수 있으며, 필요한 모든 상위 디렉토리도 함께 생성됩니다.
# exist_ok=True 파라미터는 해당 경로에 디렉토리가 이미 존재하는 경우 오류를 발생시키지 않고 넘어가도록 합니다. 즉, 디렉토리가 이미 존재하면 아무 작업도 수행하지 않고, 존재하지 않을 때만 새로 생성합니다.
# 이 명령은 파일을 저장하거나 데이터를 작업할 때 필요한 디렉토리 구조를 미리 준비하는 데 유용합니다.

# List and download all files in the S3 bucket
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=bucket_name):
    for obj in page['Contents']:
        file_name = obj['Key']
        download_path = os.path.join(download_directory, file_name.replace('/', os.sep))
        
        # Ensure the directory structure exists
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        
        # Download the file
        s3.download_file(bucket_name, file_name, download_path)
        print(f"Downloaded {file_name} to {download_path}")


# C:/workplace/airflow/llm/file 폴더 하위의 날짜별 폴더 데이터가 여러개 존재한다. aaaa.zip폴더만 복사해오고 해당 폴더만 압축해제하도록 수정

source_directory = 'C:/workplace/airflow/llm/file'
destination_directory = 'C:/desired/destination/path'  # Update this to your desired destination

# Ensure the destination directory exists
os.makedirs(destination_directory, exist_ok=True)

for root, dirs, files in os.walk(source_directory):
    for file in files:
        if file == 'aaaa.zip':
            source_path = os.path.join(root, file)
            destination_path = os.path.join(destination_directory, file)
            
            # Copy aaaa.zip to the destination directory
            shutil.copy2(source_path, destination_path)
            print(f"Copied {source_path} to {destination_path}")
            
            # Extract aaaa.zip in the destination directory
            with zipfile.ZipFile(destination_path, 'r') as zip_ref:
                zip_ref.extractall(destination_directory)
                print(f"Extracted {destination_path} in {destination_directory}")


# s3전체 파일 다운로드 하고 이미 다운로드 받은 파일과 중복되는 파일은 다운로드 되지 않도록 수정

# List and download all files in the S3 bucket
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=bucket_name):
    if 'Contents' in page:  # Check if the page has contents
        for obj in page['Contents']:
            file_name = obj['Key']
            download_path = os.path.join(download_directory, file_name.replace('/', os.sep))
            
            # Ensure the directory structure exists
            os.makedirs(os.path.dirname(download_path), exist_ok=True)
            
            # Check if the file already exists
            if os.path.exists(download_path):
                print(f"File {file_name} already exists, skipping download.")
            else:
                # Download the file
                s3.download_file(bucket_name, file_name, download_path)
                print(f"Downloaded {file_name} to {download_path}")






  ##########################
# s3에 업로드 된 파일 중 20240206과 20240205 폴더 하위에 여러 zip파일이 존재할 경우 현재일과 일치하는 폴더 하위의 zip파일만 다운로드되도록 작성
 ##########################

            # AWS S3 setup
s3 = boto3.client('s3')
bucket_name = 'your-bucket-name'  # Replace with your bucket name

# Determine the current date in the format of the folder names
current_date = datetime.now().strftime('%Y%m%d')

# The directory where you want to download the ZIP files
download_directory = 'C:/workplace/airflow/llm/file'

# Ensure the download directory exists
os.makedirs(download_directory, exist_ok=True)

# List and download ZIP files from the current date folder in the S3 bucket
for obj in s3.list_objects_v2(Bucket=bucket_name, Prefix=current_date)['Contents']:
    file_name = obj['Key']
    if file_name.endswith('.zip'):
        # Full path for the file to be downloaded
        download_path = os.path.join(download_directory, os.path.basename(file_name))
        
        # Download the file
        s3.download_file(bucket_name, file_name, download_path)
        print(f"Downloaded {file_name} to {download_path}")