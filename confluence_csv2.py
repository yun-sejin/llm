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
import re
from custom_postgres_hook2 import CustomPostgresHook2

data = [
    {"src_type": "text", "title": "Title 1", "description": "Description 1", "data": "Data 1", "create_time": "2021-01-01 00:00:00"},
    {"src_type": "text", "title": "Title 2", "description": "Description 2", "data": "Data 2", "create_time": "2021-01-01 00:00:00"},
    {"src_type": "text", "title": "Title 3", "description": "Description 3", "data": "Data 3", "create_time": "2021-01-01 00:00:00"},
    {"src_type": "text", "title": "Title 4", "description": "Description 4", "data": "Data 4", "create_time": "2021-01-01 00:00:00"},
    {"src_type": "text", "title": "Title 5", "description": "Description 5", "data": "Data 5", "create_time": "2021-01-01 00:00:00"}
]

def replace_special_characters(text):
    replacements = {
        '!': '1',
        '@': '2',
        '#': '3',
        '$': '4',
        '%': '5',
        '^': '6',
        '&': '7',
        '*': '8',
        '(': '9',
        ')': '0'
    }
    for old_char, new_char in replacements.items():
        text = text.replace(old_char, new_char)
    return text

def remove_selected_special_characters(text):
    # Define the characters to be removed
    characters_to_remove = r'[!@#$%^&*()_+=\[\]{};:"\\|,.<>/?]'
    return re.sub(characters_to_remove, '', text)

def write_data_to_csv(data):
    base_folder = Path("/tmp/confluence")
    base_folder.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(dir=base_folder))
    csv_file_path = temp_dir / 'confluence.csv'
    
    with open(csv_file_path, 'w', newline='') as csvfile:
        fieldnames = ['src_type', 'title', 'description', 'data', 'create_time']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for row in data:
            row = {k: remove_selected_special_characters(replace_special_characters(v)) if isinstance(v, str) else v for k, v in row.items()}
            writer.writerow(row)
    
    print(f"CSV file created at {csv_file_path}")
    return csv_file_path, temp_dir

def create_temp_table_with(hook, table_name):
    hook.create_temp_table(table_name)

def load_csv_to_postgres(csv_file_path, table_name, delimiter=',', header=True):
    hook = CustomPostgresHook2(postgres_conn_id=postgres_conn_id)
    hook.bulk_load(table_name, str(csv_file_path), delimiter, header, is_replace=True)
    print(f"Data from {csv_file_path} loaded into {table_name} table using CustomPostgresHook with delimiter '{delimiter}' and header={header}")

def drop_temp_table(hook, table_name):
    hook.drop_temp_table(table_name)

def delete_temp_folder(temp_dir):
    shutil.rmtree(temp_dir)
    print(f"Temporary directory {temp_dir} deleted")

if __name__ == "__main__":
    csv_file_path, temp_dir = write_data_to_csv(data)
    print(f"CSV file path: {csv_file_path}")
    
    table_name = "your_temp_table_name"  # Replace with your temporary table name
    postgres_conn_id = "your_postgres_conn_id"  # Replace with your Airflow Postgres connection ID
    
    hook = CustomPostgresHook2(postgres_conn_id=postgres_conn_id)
    
    try:
        create_temp_table_with(hook, table_name)
        load_csv_to_postgres(csv_file_path, table_name, delimiter=',', header=True)
    finally:
        drop_temp_table(hook, table_name)
        delete_temp_folder(temp_dir)
