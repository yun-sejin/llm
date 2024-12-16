# import boto3
import json
import zipfile
import os

# def download_confluence_zip(bucket_name, object_key, download_path):
#     s3 = boto3.client('s3')
#     s3.download_file(bucket_name, object_key, download_path)

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

    new_text_files = {}
    
    raw_content = []
    contents = {}
    data = {}
    
    for file_name in text_files:
        if file_name in html_files:
            html_path = html_files[file_name]
            text_path = text_files[file_name]
            
            # print(f'Reading text file: {text_path}')
            with open(text_path, 'r', encoding='utf-8') as f:
                text_content = f.read()
                 # 문자열을 딕셔너리로 변환
                try:
                    # JSON 문자열에서 "contents" 키의 값을 추출
                    start_index = text_content.find('"contents" : ') + len('"contents" : ')
                    json_string = text_content[start_index:].strip().strip('"')
                    
                    # 이스케이프 문자 제거
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
                
            # print(f'Reading HTML file: {html_path}')
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
                print(f'Content of {html_path}:')
                print(html_content)
                
                data["contents"] = html_content
                contents["data"] = data
        
        else:
            text_path = text_files[file_name]
            
            # print(f'Reading text file: {text_path}')
            with open(text_path, 'r', encoding='utf-8') as f:
                text_content = f.read()
                print(f'Content of {file_path}:')
                print(text_content)
    
                # 문자열을 딕셔너리로 변환
                try:
                    # JSON 문자열에서 "contents" 키의 값을 추출
                    json_string = text_content.strip().strip('"')
                    
                    # 이스케이프 문자 제거
                    json_string = json_string.replace('\n', '').replace('\\"', '"')
                    
                    text_content_json = json.loads(json_string)
                    print('Converted to dictionary:')
                    print(text_content_json)
                    
                    # spaceview 처리
                    spaceview = text_content_json.get("spaceview", [])
                    data = {}
                    data["page_list"] = [view.get("groupName") for view in spaceview]
                    print('Data with page_list:')
                    print(data)
                except json.JSONDecodeError as e:
                    print(f'Error decoding JSON: {e}')
        
        print(data)
        print(contents)    
        print("=====================================")
    raw_content.append(contents) 
    print(raw_content)      
                
             
            # prefix = 'confluence'
            # src_id = f'{prefix}_{text_content_json.get("id", "unknown_id")}'
            # new_text_files[file_name] = {
            #     'data': html_content,
            #     'confluence_id': src_id
            # }
                
    print(f'New text files dictionary: {new_text_files}')
    print(f'Contents object: {contents}')

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
# bucket_name = your-bucket-name'
# object_key = 'confluence2.zip'
# download_path = '/home/luemier/llm2/dags/data/confluence2.zip'
download_path = '/home/luemier/llm2/dags/data/space4.zip'
extract_to = '/home/luemier/llm2/dags/data'  # Updated extraction path
# html_file_path = os.path.join(extract_to, 'confluence', '11111.html')
zip_file_path = download_path

# print(f'Downloading {object_key} from bucket {bucket_name} to {download_path}')
# download_confluence_zip(bucket_name, object_key, download_path)
print(f'Unzipping {download_path} to {extract_to}')
unzip_file(download_path, extract_to)
print('Done.')

# html_content = read_html_file(html_file_path)
# print(html_content)

zip_contents = read_zip_file(zip_file_path)
print(zip_contents)

# if __name__ == '__main__':
#     unzip_file('/home/luemier/llm2/dags/data/confluence.zip', '/home/luemier/llm2/dags/data/')
#     html_content = read_html_file('/home/luemier/llm2/dags/data/confluence/11111.html')
#     print(html_content)
#     zip_contents = read_zip_file('/home/luemier/llm2/dags/data/confluence.zip')
#     print(zip_contents)