import json

data = {
            "employees": [
                {"name": "Alice", "age": "29", "department": "Development"},
                {"name": "Bob", "age": "32", "department": "Design"},
                {"name": "Charlie", "age": "37", "department": "Marketing"}
            ]
        }
# Convert the data to JSON format
json_data = json.dumps(data)

# Write the JSON data to a file
with open('c:/workplace/airflow/llm/data.json', 'w') as file:
    str(file.write(json_data))

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