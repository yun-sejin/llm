import json
import os
# import boto3
import uuid

def create_json_file(message, file_name, folder_path='fail'):
    # Ensure the folder exists
    os.makedirs(folder_path, exist_ok=True)
    
    # Define the full file path
    file_path = os.path.join(folder_path, file_name)
    
    # Write the JSON message to the file
    with open(file_path, 'w', encoding='utf-8') as json_file:
        json.dump(message, json_file, ensure_ascii=False, indent=4)
    
    print(f'JSON file created at {file_path}')
    return file_path

# def upload_to_s3(file_path, bucket_name, object_key):
#     s3 = boto3.client('s3')
#     unique_id = str(uuid.uuid4())
#     object_key_with_uuid = os.path.join(unique_id, object_key)
#     try:
#         s3.upload_file(file_path, bucket_name, object_key_with_uuid)
#         print(f'File {file_path} uploaded to S3 bucket {bucket_name} with key {object_key_with_uuid}')
#     except Exception as e:
#         print(f'Failed to upload {file_path} to S3: {e}')

# Example usage
message = [{
    "src_id": "qna_dsqna_1.",
    "error": "Another error occurred1.",
    "etc": [{"a":"Another error occurred1." , "b":"Another error occurred1."}]
    },{
    "src_id": "qna_dsqna_2.",
    "error": "Another error occurred2.",
    "etc": [{"a":"Another error occurred1." , "b":"Another error occurred1."}]
    },{
    "src_id": "qna_dsqna_3.",
    "error": "Another error occurred3.",
    "etc": [{"a":"Another error occurred1." , "b":"Another error occurred1."}]
    }]
file_name = 'error_message.json'
bucket_name = 'your-bucket-name'
object_key = 'path/to/error_message.json'

file_path = create_json_file(message, file_name)
# upload_to_s3(file_path, bucket_name, object_key)
