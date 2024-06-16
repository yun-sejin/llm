import json

# Read the JSON data from the file
with open('c:/workplace/airflow/llm/data.json', 'r') as file:
    json_data = file.read()

# Parse the JSON data
data = json.loads(json_data)

# Access the data
employees = data["employees"]
for employee in employees:
    name = employee["name"]
    age = employee["age"]
    department = employee["department"]
    print(f"Name: {name}, Age: {age}, Department: {department}")