import json
import zipfile
import os
import psycopg2
import requests

from llm.dags.validate import ValidateDocument
from llm.dags.validateFactory import ValidateFactory

# ...existing code...

class MailNotifier:
    @staticmethod
    def send_email(subject, body):
        # Replace with your actual mail API endpoint and parameters
        mail_api_url = "https://api.mailprovider.com/send"
        payload = {
            "subject": subject,
            "body": body,
            "to": "recipient@example.com"
        }
        response = requests.post(mail_api_url, json=payload)
        return response.status_code == 200

def unzip_file(zip_path, extract_to):
    confluence_folder = os.path.join(extract_to, 'confluence')
    if os.path.exists(confluence_folder):
        i = 1
        while os.path.exists(confluence_folder):
            confluence_folder = os.path.join(extract_to, f'confluence_{i}')
            i += 1
    os.makedirs(confluence_folder, exist_ok=True)
    print(f'Starting to extract {zip_path} to {confluence_folder}')
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(confluence_folder)
        process_extracted_files(confluence_folder)
        
    print(f'Extraction complete.')

def insert_raw_content(raw_content):
    conn = psycopg2.connect(
        dbname="your_db",
        user="your_user",
        password="your_password",
        host="your_host"
    )
    try:
        with conn:
            with open("/home/luemier/llm/llm/dags/parse.sql", "r", encoding="utf-8") as f:
                sql_content = f.read()

            # Convert named placeholders like :id to psycopg2-style %(id)s
            insert_sql = (
                sql_content
                .replace(":id", "%(id)s")
                .replace(":src_type", "%(src_type)s")
                .replace(":src_sub_type", "%(src_sub_type)s")
                .replace(":raw_content", "%(raw_content)s")
            )

            with conn.cursor() as cur:
                for idx, item in enumerate(raw_content, start=1):
                    params = {
                        "id": idx,
                        "src_type": "confluence",
                        "src_sub_type": "page",
                        "raw_content": str(item)
                    }
                    cur.execute(insert_sql, params)
    finally:
        conn.close()

def insert_failed_files(failed_files):
    conn = psycopg2.connect(
        dbname="your_db",
        user="your_user",
        password="your_password",
        host="your_host"
    )
    try:
        with conn:
            with conn.cursor() as cur:
                for failed_file in failed_files:
                    insert_sql = """
                    INSERT INTO error_logs (file_name, error_message, created_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    """
                    cur.execute(insert_sql, (failed_file["fileName"], failed_file["errMessage"]))
    finally:
        conn.close()

def process_extracted_files(confluence_folder):
    html_files = {}
    text_files = {}

    for root, dirs, files in os.walk(confluence_folder):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith('.html'):
                html_files[file.split('.')[0]] = file_path
            elif file.endswith('.txt'):
                text_files[file.split('.')[0]] = file_path
 
    raw_content = []
    
    for file_name in text_files:
        contents = {}
        data = {}
        if file_name in html_files:
            html_path = html_files[file_name]
            text_path = text_files[file_name]
            
            with open(text_path, 'r', encoding='utf-8') as f:
                text_content = f.read()
                try:
                    start_index = text_content.find('"contents" : ') + len('"contents" : ')
                    json_string = text_content[start_index:].strip().strip('"')
                    json_string = json_string.replace('\n', '').replace('\\"', '"')
                    text_content_json = json.loads(json_string)
                    print('Converted to dictionary:')
                    print(text_content_json)
                except json.JSONDecodeError as e:
                    print(f'Error decoding JSON: {e}')
                    
                contents["src_id"] = text_content_json.get("id")
                contents["id"] = text_content_json.get("id")
                data["id"] = text_content_json.get("id")
                
                if file_name != 'index':
                    ancestors = text_content_json.get("ancesotor", [])
                    contents["auth_group_list"] = [ancestor.get("title") for ancestor in ancestors]
                else:
                    spaceview = text_content_json.get("spaceview", [])
                    data["page_list"] = [view.get("groupName") for view in spaceview]
                
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
                print(f'Content of {html_path}:')
                print(html_content)
                
                data["contents"] = html_content
            
            # Convert data to bytea
            data_bytea = json.dumps(data).encode('utf-8')
            contents["data"] = data_bytea
        
        print(data)
        print(contents)
        print("json dump========================")
        print("=====================================")
        raw_content.append(contents) 
        print(raw_content)      
    
    insert_raw_content(raw_content)
    
    #1.Validat the extracted files
    doc_validator = ValidateFactory.create_validator("document")
    mismatched = doc_validator.validate_mismatched_pairs()
    print("Mismatched pairs:", mismatched)
    
    # Example usage
    confluence_folder = '/home/luemier/llm/llm/dags/data'
    validator = ValidateDocument()
    is_valid, failed_files, valid_files = validator.validate_html_txt_pairs(confluence_folder)

    if is_valid:
        insert_raw_content(valid_files)
    else:
        insert_raw_content(valid_files)
        # insert_failed_files(failed_files)
        MailNotifier.send_email(
            subject="Validation Failed",
            body=f"Failed files: {failed_files}"
        )

    print(f'Validation result: {is_valid}')
    print(f'Failed files: {failed_files}')
    print(f'Valid files: {valid_files}')

    #2.validate text files
    # Example usage
    file_paths = [
        '/home/luemier/llm/llm/dags/data/11111.txt',
        '/home/luemier/llm/llm/dags/data/11112.txt',
        # Add more file paths as needed
    ]
    required_keys = ["src_id", "dag"]
    validator = ValidateDocument()
    is_valid, failed_files = validator.validate_multiple_json_txt_files(file_paths, required_keys)

    print(f'Validation result: {is_valid}')
    print(f'Failed files: {failed_files}')

    
    return raw_content

def read_html_file(file_path):
    print(f'Reading HTML file from {file_path}')
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    print(f'Finished reading HTML file.')
    return content

def read_zip_file(zip_path):
    print(f'Reading ZIP file from {zip_path}')
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        file_list = zip_ref.namelist()
    print(f'Finished reading ZIP file. Contents: {file_list}')
    return file_list

# Example usage
download_path = '/home/luemier/llm2/dags/data/space4.zip'
extract_to = '/home/luemier/llm2/dags/data'  # Updated extraction path

print(f'Unzipping {download_path} to {extract_to}')
unzip_file(download_path, extract_to)
print('Done.')


#validate the extracted files
required_keys = ["spaceid", "spacename", "type"]
file_name = 'info.txt'
validator2 = ValidateFactory.create_validator("document")
validator2.initialize(extract_to, file_name, required_keys)
validator2.validate_html_txt_pairs(download_path)
validator = ValidateFactory.create_validator("file")
validator.initialize(extract_to, file_name, required_keys)
is_valid, err_message, failed_file = validator.validate_info_txt()

fail_files = {"fail_file_name": [], "err_message": ""}
if not is_valid:
    fail_files["fail_file_name"].append(failed_file)
    fail_files["err_message"] = err_message

print(f'Validation result: {is_valid}')
print(f'Fail files: {fail_files}')

# Validate contents file pairs in the zip file
is_valid_pairs, err_message_pairs, missing_files = validator.validate_contents_file_pairs(download_path)
if not is_valid_pairs:
    fail_files["fail_file_name"].extend(missing_files)
    fail_files["err_message"] = err_message_pairs

print(f'Validation result for contents file pairs: {is_valid_pairs}')
print(f'Fail files: {fail_files}')

# Validate at least one file exists in the zip file
is_valid_files, err_message_files = validator.validate_at_least_one_file(download_path)
if not is_valid_files:
    fail_files["fail_file_name"].append(download_path)
    fail_files["err_message"] = err_message_files

print(f'Validation result for at least one file: {is_valid_files}')
print(f'Fail files: {fail_files}')

# Validate info.txt exists in the zip file
is_valid_info_txt, err_message_info_txt = validator.validate_info_txt_exists(download_path)
if not is_valid_info_txt:
    fail_files["fail_file_name"].append(download_path)
    fail_files["err_message"] = err_message_info_txt

print(f'Validation result for info.txt existence: {is_valid_info_txt}')
print(f'Fail files: {fail_files}')

# Validate txt files for missing keys
is_valid_txt, err_message_txt, failed_txt_files = validator2.validate_txt_files(extract_to)
if not is_valid_txt:
    fail_files["fail_file_name"].extend(failed_txt_files)
    fail_files["err_message"] = err_message_txt

print(f'Validation result for txt files: {is_valid_txt}')
print(f'Fail files: {fail_files}')

# Validate html files for empty content
is_valid_html, err_message_html, failed_html_files = validator2.validate_html_files(extract_to)
if not is_valid_html:
    fail_files["fail_file_name"].extend(failed_html_files)
    fail_files["err_message"] = err_message_html

print(f'Validation result for html files: {is_valid_html}')
print(f'Fail files: {fail_files}')

# ...existing code...
