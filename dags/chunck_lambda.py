import json
import os

def extract_invalid_src_id(data):
    invalid_data = []
    try:
        for item in data:
            if 'src_id' not in item or not item['src_id']:
                invalid_data.append(item)
    except Exception as e:
        print(f"Failed to process data: {e}")
    return invalid_data

# Lambda expression to extract invalid src_id
extract_invalid_src_id = lambda data: [item for item in data if 'src_id' not in item or not item['src_id']]


def save_invalid_data(invalid_data, output_path):
    try:
        with open(output_path, 'w', encoding='utf-8') as file:
            json.dump(invalid_data, file, indent=4)
        print(f"Invalid data saved to {output_path}")
    except Exception as e:
        print(f"Failed to save invalid data: {e}")

# Example usage
data = [
    {"src_id": "1", "name": "Item 1"},
    {"name": "Item 2"},
    {"src_id": "3", "name": "Item 3"},
    {"src_id": None, "name": "Item 4"},
    {"src_id": "5", "name": "Item 5"},
    {"src_id": "", "name": "Item 6"},
    {"src_id": "7", "name": "Item 7"},
    {"src_id": "8", "name": "Item 8"},
    {"src_id": "9", "name": "Item 9"},
    {"src_id": "10", "name": "Item 10"}
]

output_file_path = '/path/to/your/output_file.json'

invalid_data = extract_invalid_src_id(data)
save_invalid_data(invalid_data, output_file_path)
