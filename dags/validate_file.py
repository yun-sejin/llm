import json
import os

class ErrorMessage:
    def __init__(self, file_name, error_message):
        self.file_name = file_name
        self.error_message = error_message

    def to_dict(self):
        return {"file_name": self.file_name, "error_message": self.error_message}

class FileValidator:
    required_keys = {
        "": {"src_id", "src_type", "src_sub_type", "auth_group_lsit", "data", "createtime", "wgt_param"},
        "data": {"name", "createid", "modifiedid", "modifiedtime", "dag", "tags", "add_info"},
        "data.dag": {"tasks"},
        "data.dag.tasks": {"task_id", "operator", "bash_command"}
    }
    
    def __init__(self, file_path):
        self.file_path = file_path
    
    def validate(self):
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                
                # Check all required keys and their values
                for key_path, required_keys in self.required_keys.items():
                    keys = key_path.split('.') if key_path else []
                    nested_data = data
                    for key in keys:
                        nested_data = nested_data.get(key, {})
                    if isinstance(nested_data, list):
                        for item in nested_data:
                            missing_keys = required_keys - item.keys()
                            empty_values = {k for k, v in item.items() if not v}
                            if missing_keys or empty_values:
                                error_message = f"Missing or empty keys in {key_path}: {missing_keys.union(empty_values)}"
                                print(error_message)
                                return ErrorMessage(os.path.basename(self.file_path), error_message).to_dict()
                    else:
                        missing_keys = required_keys - nested_data.keys()
                        empty_values = {k for k, v in nested_data.items() if not v}
                        if missing_keys or empty_values:
                            error_message = f"Missing or empty keys in {key_path}: {missing_keys.union(empty_values)}"
                            print(error_message)
                            return ErrorMessage(os.path.basename(self.file_path), error_message).to_dict()
                
                print("File format is valid.")
                return ErrorMessage(os.path.basename(self.file_path), None).to_dict()
        
        except Exception as e:
            error_message = f"Failed to validate file: {e}"
            print(error_message)
            return ErrorMessage(os.path.basename(self.file_path), error_message).to_dict()

# Example usage
file_path = '/home/luemier/llm/llm/dags/data/11111.txt'
validator = FileValidator(file_path)
validation_result = validator.validate()
if validation_result["error_message"] is None:
    print("File format is correct.")
else:
    print(f"File format is incorrect: {validation_result['error_message']}")
