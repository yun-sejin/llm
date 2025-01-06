import zipfile
import os

def is_zip_file_valid(zip_path):
    if not zipfile.is_zipfile(zip_path):
        return {"file_name": os.path.basename(zip_path), "error_message": "Not a valid ZIP file"}
    
    if os.path.getsize(zip_path) == 0:
        return {"file_name": os.path.basename(zip_path), "error_message": "ZIP file is empty"}
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            bad_file = zip_ref.testzip()
            if bad_file:
                return {"file_name": os.path.basename(zip_path), "error_message": f"Corrupted file found: {bad_file}"}
        return {"file_name": os.path.basename(zip_path), "error_message": None}
    except Exception as e:
        return {"file_name": os.path.basename(zip_path), "error_message": f"Failed to validate ZIP file: {e}"}

# Example usage
zip_path = '/home/luemier/llm/llm/dags/data/sample.zip'
validation_result = is_zip_file_valid(zip_path)
if validation_result["error_message"] is None:
    print("ZIP file is valid.")
else:
    print(f"ZIP file is invalid: {validation_result['error_message']}")
