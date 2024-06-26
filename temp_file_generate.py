import os
import zipfile

# Variable for the folder name
folder_name = "aaa"

# Base directory where the folder is located
base_dir = "C:/workplace/airflow/llm/file"

# Full path to the target folder
target_folder = os.path.join(base_dir, folder_name)

# Iterate through each file in the target folder
for file in os.listdir(target_folder):
    if file.endswith(".zip"):
        # Full path to the zip file
        zip_file_path = os.path.join(target_folder, file)
        
        # Unzip the file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(target_folder)

        print(f"Extracted {file} in {target_folder}")