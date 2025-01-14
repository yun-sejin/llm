from airflow.decorators import dag, task, short_circuit
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os
import zipfile
import json
import boto3
import uuid
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    description='A simple DAG to parse and validate Confluence data',
    schedule_interval=timedelta(days=1),
    catchup=False,
)
def confluence_parse_dag():
    
    @short_circuit
    @task
    def check_files_uploaded_on_date(execution_date):
        s3 = boto3.client('s3')
        bucket_name = 'confluence'
        date_prefix = f'upload/{execution_date.strftime("%Y%m%d")}/'
        
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=date_prefix, Delimiter='/')
        if 'CommonPrefixes' in response:
            uuid_prefixes = [prefix['Prefix'] for prefix in response['CommonPrefixes']]
            if uuid_prefixes:
                print(f'UUID folders found: {uuid_prefixes}')
                files = []
                for uuid_prefix in uuid_prefixes:
                    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=uuid_prefix)
                    if 'Contents' in response:
                        files.extend([obj['Key'] for obj in response['Contents']])
                if files:
                    print(f'Files found: {files}')
                    return files
        print('No files found for the given date.')
        return []

    @task
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
        print(f'Extraction complete.')

    @task
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

    @task
    def validate_files(confluence_folder):
        required_keys = ["id", "contents", "ancesotor", "url"]
        failed_files = []
        for root, dirs, files in os.walk(confluence_folder):
            for file in files:
                if file.endswith('.txt'):
                    file_path = os.path.join(root, file)
                    with open(file_path, 'r', encoding='utf-8') as f:
                        text_content = f.read()
                        try:
                            start_index = text_content.find('"contents" : ') + len('"contents" : ')
                            json_string = text_content[start_index:].strip().strip('"')
                            json_string = json_string.replace('\n', '').replace('\\"', '"')
                            text_content_json = json.loads(json_string)
                            missing_keys = [key for key in required_keys if key not in text_content_json]
                            if missing_keys:
                                print(f'Missing keys {missing_keys} in {file}')
                                failed_files.append(file)
                        except json.JSONDecodeError as e:
                            print(f'Error decoding JSON in {file}: {e}')
                            failed_files.append(file)
                elif file.endswith('.html'):
                    file_path = os.path.join(root, file)
                    with open(file_path, 'r', encoding='utf-8') as f:
                        html_content = f.read()
                        if not html_content.strip():
                            print(f'HTML content is empty in {file}')
                            failed_files.append(file)
        if failed_files:
            return False, f'Validation failed for files: {failed_files}', failed_files
        return True, "", []

    @task
    def update_history_table(files, execution_date):
        # Generate a new UUID
        job_id = str(uuid.uuid4())
        
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname='your_dbname',
            user='your_username',
            password='your_password',
            host='your_host',
            port='your_port'
        )
        cursor = conn.cursor()
        
        # Insert a new record into the history table
        cursor.execute("""
            INSERT INTO history (job_id, job_type, end_point, file_path_list, create_time, success_yn)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            job_id,
            's3',
            'confluence/upload',
            json.dumps([{"file_list": files}]),
            datetime.now(),
            True
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f'Inserted new record into history table with job_id: {job_id}')

    execution_date = '{{ ds }}'
    files = check_files_uploaded_on_date(execution_date)
    
    no_files_found = EmptyOperator(task_id='no_files_found', trigger_rule='all_done')
    update_history_task = update_history_table(files, execution_date)
    
    unzip_task = unzip_file('/home/luemier/llm2/dags/data/space4.zip', '/home/luemier/llm2/dags/data')
    process_task = process_extracted_files('/home/luemier/llm2/dags/data/confluence')
    validate_task = validate_files('/home/luemier/llm2/dags/data/confluence')

    files >> update_history_task >> unzip_task >> process_task >> validate_task
    files >> no_files_found

confluence_parse_dag = confluence_parse_dag()
