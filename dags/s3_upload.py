import boto3
import uuid
import os
from datetime import datetime

def generate_uuid():
    return str(uuid.uuid4())

def create_zip_file(source_folder, zip_filename):
    import zipfile
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for root, dirs, files in os.walk(source_folder):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, os.path.relpath(file_path, source_folder))

def upload_to_s3(bucket_name, zip_filename, s3_key):
    s3_client = boto3.client('s3')
    s3_client.upload_file(zip_filename, bucket_name, s3_key)

def main():
    source_folder = '/path/to/source/folder'
    bucket_name = 'your-s3-bucket-name'
    date_str = datetime.now().strftime('%Y%m%d')
    uuid_str = generate_uuid()
    zip_filename = f'/path/to/temp/{date_str}_{uuid_str}.zip'
    
    create_zip_file(source_folder, zip_filename)
    
    s3_key = f'{date_str}/{uuid_str}/{uuid_str}.zip'
    upload_to_s3(bucket_name, zip_filename, s3_key)
    
    print(f'Uploaded {zip_filename} to s3://{bucket_name}/{s3_key}')

if __name__ == "__main__":
    main()
