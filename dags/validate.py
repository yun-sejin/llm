import json
import zipfile
import os

from llm.dags.confluence_text import ValidateFactory



class ValidateDocument(ValidateFactory):
    """
    A class used to validate documents extracted from a zip file.
    Methods
    -------
    initialize(extract_to, file_name, required_keys)
        Initializes the validation parameters.
    validate_required_info_txt()
        Validates that all required keys are present in the info.txt file.
    validate_html_txt_pairs(zip_path)
        Validates that each HTML file has a corresponding TXT file in the zip archive.
    validate_required_keys(json_content)
        Validates that all required keys are present in the provided JSON content.
    validate_txt_files(confluence_folder)
        Validates that all TXT files in the specified folder contain the required keys.
    validate_html_files(confluence_folder)
        Validates that all HTML files in the specified folder are not empty.
    """
    def __init__(self):
        self.extract_to = None
        self.file_name = None
        self.required_keys = None

    def initialize(self, extract_to, file_name, required_keys):
        """
        Initialize the validation process with the given parameters.

        Args:
            extract_to (str): The directory where the files will be extracted to.
            file_name (str): The name of the file to be validated.
            required_keys (list): A list of keys that are required to be present in the file.

        """
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

    def validate_html_txt_pairs(self, confluence_folder):
        html_files = set()
        txt_files = set()
        failed_files = []
        valid_files = []

        info_txt_path = None
        for root, dirs, files in os.walk(confluence_folder):
            for file in files:
                if file.endswith('.html'):
                    html_files.add(file.split('.')[0])
                elif file.endswith('.txt'):
                    txt_files.add(file.split('.')[0])
                elif file == 'info.txt':
                    info_txt_path = os.path.join(root, file)

        only_html = html_files - txt_files
        only_txt = txt_files - html_files

        for file_name in only_html:
            failed_files.append({"fileName": f"{file_name}.html", "errMessage": "No matching .txt file"})
        for file_name in only_txt:
            failed_files.append({"fileName": f"{file_name}.txt", "errMessage": "No matching .html file"})

        if not os.path.isfile(info_txt_path):
            failed_files.append({"fileName": "info.txt", "errMessage": "info.txt file does not exist"})

        valid_files = list(html_files & txt_files)

        if failed_files:
            return False, failed_files, valid_files
        return True, failed_files, valid_files

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
                            json_string = text_content[start_index:].trip().strip('"')
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

    def validate_raw_content(self, raw_content):
        if not raw_content:
            return False, "No raw content to validate"
        # Add any further checks needed here
        return True, ""

    def validate_mismatched_pairs(confluence_folder):
        html_files = set()
        txt_files = set()
        
        for root, dirs, files in os.walk(confluence_folder):
            for file in files:
                if file.endswith('.html'):
                    html_files.add(file.split('.')[0])
                elif file.endswith('.txt'):
                    txt_files.add(file.split('.')[0])

        mismatched = {}
        only_html = html_files - txt_files
        only_txt = txt_files - html_files

        for file_name in only_html:
            mismatched[file_name] = {
                "fileName": f"{file_name}.html",
                "errMessage": "No matching .txt file"
            }
        for file_name in only_txt:
            mismatched[file_name] = {
                "fileName": f"{file_name}.txt",
                "errMessage": "No matching .html file"
            }

        return mismatched

    def validate_json_file(self, json_filepath, required_keys):
        if not os.path.isfile(json_filepath):
            return False, f"No file found at {json_filepath}"

        with open(json_filepath, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                return False, f"Invalid JSON in {json_filepath}: {str(e)}"
            
        missing_keys = [key for key in required_keys if key not in data]
        if missing_keys:
            return False, f"Missing keys {missing_keys} in {json_filepath}"
        return True, ""

    def validate_html_file_keys(self, html_filepath, required_keys):
        if not os.path.isfile(html_filepath):
            return False, f"No file found at {html_filepath}"

        with open(html_filepath, 'r', encoding='utf-8') as f:
            html_content = f.read()

        missing_keys = [key for key in required_keys if key not in html_content]
        if missing_keys:
            return False, f"Missing keys {missing_keys} in {html_filepath}"
        return True, ""

    def validate_json_txt_files(self, confluence_folder, required_keys):
        failed_files = []
        for root, dirs, files in os.walk(confluence_folder):
            for file in files:
                if file.endswith('.txt'):
                    file_path = os.path.join(root, file)
                    with open(file_path, 'r', encoding='utf-8') as f:
                        try:
                            text_content = f.read()
                            json_content = json.loads(text_content)
                            missing_keys = [key for key in required_keys if key not in json_content]
                            if missing_keys:
                                print(f'Missing keys {missing_keys} in {file}')
                                failed_files.append(file)
                        except json.JSONDecodeError as e:
                            print(f'Error decoding JSON in {file}: {e}')
                            failed_files.append(file)
        if failed_files:
            return False, failed_files
        return True, []

    def validate_multiple_json_txt_files(self, file_paths, required_keys):
        failed_files = []
        for file_path in file_paths:
            if not os.path.isfile(file_path):
                failed_files.append({"fileName": file_path, "errMessage": "File not found"})
                continue

            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    text_content = f.read()
                    json_content = json.loads(text_content)
                    missing_keys = [key for key in required_keys if key not in json_content]
                    if missing_keys:
                        failed_files.append({"fileName": file_path, "errMessage": f"Missing keys {missing_keys}"})
                except json.JSONDecodeError as e:
                    failed_files.append({"fileName": file_path, "errMessage": f"Error decoding JSON: {str(e)}"})
        
        if failed_files:
            return False, failed_files
        return True, []

    def validate_html_txt_pairs_and_info(self, confluence_folder):
        html_files = set()
        txt_files = set()
        failed_files = []

        for root, dirs, files in os.walk(confluence_folder):
            for file in files:
                if file.endswith('.html'):
                    html_files.add(file.split('.')[0])
                elif file.endswith('.txt'):
                    txt_files.add(file.split('.')[0])
                elif file == 'info.txt':
                    info_txt_path = os.path.join(root, file)

        only_html = html_files - txt_files
        only_txt = txt_files - html_files

        for file_name in only_html:
            failed_files.append({"fileName": f"{file_name}.html", "errMessage": "No matching .txt file"})
        for file_name in only_txt:
            failed_files.append({"fileName": f"{file_name}.txt", "errMessage": "No matching .html file"})

        if not os.path.isfile(info_txt_path):
            failed_files.append({"fileName": "info.txt", "errMessage": "info.txt file does not exist"})

        if failed_files:
            return False, failed_files
        return True, []

    def validate_files_for_keys_and_values(self, file_paths, required_keys):
        failed_files = []
        for file_path in file_paths:
            if not os.path.isfile(file_path):
                failed_files.append({"fileName": file_path, "errMessage": "File not found"})
                continue

            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    text_content = f.read()
                    json_content = json.loads(text_content)
                    missing_keys = [key for key in required_keys if key not in json_content or not json_content[key]]
                    if missing_keys:
                        failed_files.append({"fileName": file_path, "errMessage": f"Missing keys or values {missing_keys}"})
                except json.JSONDecodeError as e:
                    failed_files.append({"fileName": file_path, "errMessage": f"Error decoding JSON: {str(e)}"})
        
        if failed_files:
            return False, failed_files
        return True, []

    def validate_files(self, file_keys):
        """
        Validate the given file keys.
        
        Args:
            file_keys (list): List of file keys to validate.
         
        Returns:
            tuple: (bool, str) indicating if all files are valid and an error message if not.
        """
        for file_key in file_keys:
            if not file_key.endswith('.zip'):
                return False, f"Invalid file type for {file_key}"
        return True, ""

class ValidateFile(ValidateFactory):
    def __init__(self):
        self.extract_to = None
        self.file_name = None
        self.required_keys = None

    def initialize(self, extract_to, file_name, required_keys):
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