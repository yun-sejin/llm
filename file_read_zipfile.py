import json
import os
import zipfile
import psycopg2

def unzip_files():
        #zip_path = '/path/to/your/zip/file.zip'
        #extract_path = '/path/to/extract/files'
        
        zip_path = 'c:/workplace/airflow/llm/file.zip'
        extract_path = 'c:/workplace/airflow/llm/extract/files'

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        
        return extract_path

#def load_json_files():
extract_path = os.path.join(unzip_files(), 'file')
json_files = [file for file in os.listdir(extract_path) if file.endswith('.json')]

if json_files:
    conn = psycopg2.connect(
        host='your_host',
        port='your_port',
        database='your_database',
        user='your_user',
        password='your_password'
    )
            
    cursor = conn.cursor()
            
    for file in json_files:
        file_path = os.path.join(extract_path, file)
                
        with open(file_path, 'r') as json_file:
            json_data = json_file.read()

        # Parse the JSON data
        data = json.loads(json_data)

        # Access the data
        employees = data["employees"]
        for employee in employees:
            name = employee["name"]
            age = employee["age"]
            department = employee["department"]
            print(f"Name: {name}, Age: {age}, Department: {department}")

        cursor.execute("""
            INSERT INTO dll_raw (name, age, department)
            VALUES (%s, %s, %s)
        """, (name, age, department))
            
    conn.commit()
    cursor.close()
    conn.close() 
else:
    print('No JSON files found in the extracted folder.')
    
   