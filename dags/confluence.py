# import boto3
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
    json_files = {}

    for root, dirs, files in os.walk(confluence_folder):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith('.html'):
                html_files[file.split('.')[0]] = file_path
            elif file.endswith('.json'):
                json_files[file.split('.')[0]] = file_path

    for file_name in html_files:
        if file_name in json_files:
            html_path = html_files[file_name]
            json_path = json_files[file_name]
            print(f'Reading HTML file: {html_path}')
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
                print(f'Content of {html_path}:')
                print(html_content)
            
            print(f'Reading JSON file: {json_path}')
            with open(json_path, 'r', encoding='utf-8') as f:
                json_content = f.read()
                print(f'Content of {json_path}:')
                print(json_content)

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
bucket_name = 'your-bucket-name'
object_key = 'confluence.zip'
download_path = '/home/luemier/llm2/dags/data/confluence.zip'
extract_to = '/home/luemier/llm2/dags/data2'  # Updated extraction path
html_file_path = os.path.join(extract_to, 'confluence', '11111.html')
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
