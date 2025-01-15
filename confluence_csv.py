# import boto3
import os
from pathlib import Path
import zipfile
import json
import csv
import tempfile
import subprocess
import requests
import shutil

def download_file_from_s3(bucket_name, object_key, dest_folder):
    ...
    # dest_folder = Path(dest_folder)
    # if not dest_folder.exists():
    #     dest_folder.mkdir(parents=True)
    # s3 = boto3.client('s3')
    # file_path = dest_folder / 'space4.zip'
    # s3.download_file(bucket_name, object_key, str(file_path))
    # print(f"Downloaded {file_path}")
    # return file_path

def extract_zip_file(zip_file_path, extract_to_folder):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)
    print(f"Extracted {zip_file_path} to {extract_to_folder}")

def validate_txt_data(txt_data):
    required_keys = ["title", "description"]
    for key in required_keys:
        if key not in txt_data:
            error_msg = f"Validation failed: Missing key {key}"
            print(error_msg)
            return {"is_success": False, "error_msg": error_msg}
    return {"is_success": True, "error_msg": ""}

def combine_matching_files_to_list(extracted_folder):
    all_data = []
    fail_data = []
    files_dict = {}
    
    for file_path in Path(extracted_folder).rglob('*'):
        if file_path.is_file():
            file_name = file_path.stem
            file_ext = file_path.suffix
            if file_name not in files_dict:
                files_dict[file_name] = {}
            files_dict[file_name][file_ext] = file_path
    
    for file_name, paths in files_dict.items():
        if '.txt' in paths and '.html' in paths:
            with open(paths['.txt'], 'r') as txt_file:
                txt_data = json.load(txt_file)
            validation_result = validate_txt_data(txt_data)
            if validation_result["is_success"]:
                with open(paths['.html'], 'r') as html_file:
                    html_content = html_file.read()
                
                combined_data = {
                    "id": txt_data["title"],
                    "src_type": txt_data["description"],
                    "data": html_content
                }
                all_data.append(combined_data)
            else:
                fail_data.append({"file_name": file_name, "error_msg": validation_result["error_msg"]})
                print(f"Skipping file {file_name} due to validation error: {validation_result['error_msg']}")
    
    print(f"Combined data from {len(all_data)} matching file pairs")
    return all_data, fail_data

def write_data_to_csv(all_data, zip_file_path):
    temp_dir = Path(tempfile.mkdtemp())
    zip_file_name = Path(zip_file_path).stem
    csv_file_path = temp_dir / f'{zip_file_name}.csv'
    
    with open(csv_file_path, 'w', newline='') as csvfile:
        fieldnames = ['id', 'src_type', 'data']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for data in all_data:
            writer.writerow(data)
    
    print(f"CSV file created at {csv_file_path}")
    return csv_file_path, temp_dir

def load_csv_with_psql(csv_file_path, db_config):
    psql_command = (
        f"psql -h {db_config['host']} -p {db_config['port']} -U {db_config['user']} -d {db_config['dbname']} "
        f"-c \"\\copy dsllm_raw(id, src_type, data) FROM '{csv_file_path}' DELIMITER ',' CSV HEADER;\""
    )
    env = os.environ.copy()
    env['PGPASSWORD'] = db_config['password']
    subprocess.run(psql_command, shell=True, env=env, check=True)
    print(f"Data from {csv_file_path} loaded into dsllm_raw table using psql")

def send_fail_data_email(fail_data, email_api_url, email_api_key, recipient_email):
    subject = "Failed Data Notification"
    body = "The following files failed validation:\n\n"
    for item in fail_data:
        body += f"File: {item['file_name']}, Error: {item['error_msg']}\n"
    
    payload = {
        "to": recipient_email,
        "subject": subject,
        "body": body
    }
    headers = {
        "Authorization": f"Bearer {email_api_key}",
        "Content-Type": "application/json"
    }
    response = requests.post(email_api_url, json=payload, headers=headers)
    if response.status_code == 200:
        print("Failed data email sent successfully")
    else:
        print(f"Failed to send email: {response.status_code} - {response.text}")

if __name__ == "__main__":
    bucket_name = "your-bucket-name"  # Replace with your S3 bucket name
    object_key = "path/to/space4.zip"  # Replace with the S3 object key
    dest_folder = Path("/home/luemier/llm/llm/dags/data2")
    # zip_file_path = download_file_from_s3(bucket_name, object_key, dest_folder)
    zip_file_path = dest_folder / "space4.zip"
    extract_zip_file(zip_file_path, dest_folder)
    all_data, fail_data = combine_matching_files_to_list(dest_folder)
    csv_file_path, temp_dir = write_data_to_csv(all_data, zip_file_path)
    
    db_config = {
        'dbname': 'your_dbname',
        'user': 'your_username',
        'password': 'your_password',
        'host': 'your_host',
        'port': 'your_port'
    }
    
    try:
        load_csv_with_psql(csv_file_path, db_config)
    finally:
        shutil.rmtree(temp_dir)
        print(f"Temporary directory {temp_dir} deleted")
    
    print(f"CSV file path: {csv_file_path}")
    print(f"Failed data: {fail_data}")
    
    email_api_url = "https://api.your-email-service.com/send"  # Replace with your email API URL
    email_api_key = "your-email-api-key"  # Replace with your email API key
    recipient_email = "recipient@example.com"  # Replace with the recipient's email address
    send_fail_data_email(fail_data, email_api_url, email_api_key, recipient_email)
