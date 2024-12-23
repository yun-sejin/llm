import json
import zipfile
import os

# ...existing code...

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
            # bytea_str = f"\\x{data_bytea.hex()}"
            contents["data"] = data_bytea
        
        print(data)
        print(contents)
        print("json dump========================")
        print("=====================================")
        raw_content.append(contents) 
        print(raw_content)      

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

class ValidateFactory:
    @staticmethod
    def create_validator(validator_type, extract_to, file_name, required_keys):
        if validator_type == "document":
            return ValidateDocument(extract_to, file_name, required_keys)
        elif validator_type == "file":
            return ValidateFile(extract_to, file_name, required_keys), file_name
        else:
            raise ValueError(f"Unknown validator type: {validator_type}")

class ValidateDocument(ValidateFactory):
    def __init__(self, extract_to, file_name, required_keys):
        self.extract_to = extract_to
        self.file_name = file_name
        self.required_keys = required_keys

    def validate_required_info_txt(self):
        
        info_txt_path = os.path.join(self.extract_to, self.file_name)
        
        with open(info_txt_path, 'r', encoding='utf-8') as f:
            info_content = f.read()
            info_json = json.loads(info_content)
            
            for key in self.required_keys:
                if key not in info_json:
                    print(f'Key {key} is missing in {self.file_name}.')
                    return False, f'Key {key} is missing', self.file_name
        
        print(f'All required keys are present in {self.file_name}.')
        return True, "", self.file_name

    def validate_html_txt_pairs(self, zip_path):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            html_files = set()
            txt_files = set()
            
            for file in file_list:
                if file == 'info.txt':
                    continue
                if file.endswith('.html'):
                    html_files.add(file.split('.')[0])
                elif file.endswith('.txt'):
                    txt_files.add(file.split('.')[0])
            
            missing_pairs = html_files.symmetric_difference(txt_files)
            if missing_pairs:
                print(f'Missing pairs: {missing_pairs}')
                return False, f'Missing pairs: {missing_pairs}', list(missing_pairs)
        
        print('All HTML and TXT files are properly paired.')
        return True, "", []

    def validate_required_keys(self, json_content):
        for key in self.required_keys:
            if key not in json_content:
                print(f'Key {key} is missing in the JSON content.')
                return False, f'Key {key} is missing', self.file_name
        print('All required keys are present in the JSON content.')
        return True, ""

    def validate_txt_files(self, confluence_folder):
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
        if failed_files:
            return False, f'Missing keys in files: {failed_files}', failed_files
        return True, "", []

    def validate_html_files(self, confluence_folder):
        failed_files = []
        for root, dirs, files in os.walk(confluence_folder):
            for file in files:
                if file.endswith('.html'):
                    file_path = os.path.join(root, file)
                    with open(file_path, 'r', encoding='utf-8') as f:
                        html_content = f.read()
                        if not html_content.strip():
                            print(f'HTML content is empty in {file}')
                            failed_files.append(file)
        if failed_files:
            return False, f'HTML content is empty in files: {failed_files}', failed_files
        return True, "", []

class ValidateFile(ValidateFactory):
    def __init__(self, extract_to, file_name, required_keys):
        self.extract_to = extract_to
        self.file_name = file_name
        self.required_keys = required_keys

    def validate_info_txt_exists(self, zip_path):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            if 'info.txt' not in zip_ref.namelist():
                print('info.txt file does not exist in the zip archive.')
                return False, 'info.txt file does not exist in the zip archive.'
        
        print('info.txt file exists in the zip archive.')
        return True, ''
    
    def validate_info_txt(self):
        # if self.file_name != 'info.txt':
        #     print(f'File name {self.file_name} does not match info.txt.')
        #     return False, "File name does not match info.txt", self.file_name
        
        
        info_txt_path = os.path.join(self.extract_to, self.file_name)
        # if not os.path.exists(info_txt_path):
        #     print(f'{info_txt_path} does not exist.')
        #     return False, "File does not exist", self.file_name
        
        with open(info_txt_path, 'r', encoding='utf-8') as f:
            info_content = f.read()
            info_json = json.loads(info_content)
            
            for key in self.required_keys:
                if key not in info_json:
                    print(f'Key {key} is missing in {self.file_name}.')
                    return False, f'Key {key} is missing', self.file_name
        
        print(f'All required keys are present in {self.file_name}.')
        return True, "", self.file_name

    def validate_contents_file_pairs(self, zip_path):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            html_files = set()
            txt_files = set()
            
            for file in file_list:
                if file == 'info.txt':
                    continue
                if file.endswith('.html'):
                    html_files.add(file.split('.')[0])
                elif file.endswith('.txt'):
                    txt_files.add(file.split('.')[0])
            
            missing_pairs = html_files.symmetric_difference(txt_files)
            if missing_pairs:
                print(f'Missing pairs: {missing_pairs}')
                return False, f'Missing pairs: {missing_pairs}', list(missing_pairs)
        
        print('All HTML and TXT files are properly paired.')
        return True, "", []

    def validate_at_least_one_file(self, zip_path):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            if not file_list:
                print('No files found in the zip archive.')
                return False, 'No files found in the zip archive.'
        
        print('At least one file exists in the zip archive.')
        return True, ''

   

# Example usage
download_path = '/home/luemier/llm2/dags/data/space4.zip'
extract_to = '/home/luemier/llm2/dags/data'  # Updated extraction path

print(f'Unzipping {download_path} to {extract_to}')
unzip_file(download_path, extract_to)
print('Done.')

required_keys = ["spaceid", "spacename", "type"]
file_name = 'info.txt'
validator2 = ValidateFactory.create_validator("document", extract_to, file_name, required_keys)
validator, file_name = ValidateFactory.create_validator("file", extract_to, file_name, required_keys)
validator2.validate_file_pairs(download_path, file_name)
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
