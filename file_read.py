import json

# JSON 문자열
json_str = '{"employees": [{"name": "Alice", "age": "29", "department": "Development"}, {"name": "Bob", "age": "32", "department": "Design"}, {"name": "Charlie", "age": "37", "department": "Marketing"}]}'

# JSON 문자열을 파이썬 객체로 변환
data = json.loads(json_str)

# 직원들의 정보를 출력
for employee in data['employees']:
    print(f"Name: {employee['name']}, Age: {employee['age']}, Department: {employee['department']}")
