import json


data = b'{"id": 33333, "contents": "<!DOCTYPE html>\\n<html>\\n<head>\\n    <title>Document</title>\\n</head>\\n<body>\\n    <h1>Welcome to My Website</h1>\\n    <p>This is a sample paragraph with some placeholder text.</p>\\n    <ul>\\n        <li>Item 1</li>\\n        <li>Item 2</li>\\n        <li>Item 3</li>\\n    </ul>\\n\\n    <table border=\\"1\\">\\n        <tr>\\n            <th>Header 1</th>\\n            <th>Header 2</th>\\n            <th>Header 3</th>\\n        </tr>\\n        <tr>\\n            <td>Row 1, Cell 1</td>\\n            <td>Row 1, Cell 2</td>\\n            <td>Row 1, Cell 3</td>\\n        </tr>\\n        <tr>\\n            <td>Row 2, Cell 1</td>\\n            <td>Row 2, Cell 2</td>\\n            <td>Row 2, Cell 3</td>\\n        </tr>\\n    </table>\\n</body>\\n</html>"}'

decoded_data = data.decode('utf-8')

json_data = json.loads(decoded_data)
json_data["aaaa"] = json_data.get("id")
print(json_data)


# import psycopg2
# import json

# # PostgreSQL 데이터베이스 연결 설정
# conn = psycopg2.connect(
#     dbname="your_dbname",
#     user="your_username",
#     password="your_password",
#     host="your_host",
#     port="your_port"
# )

# # 커서 생성
# cur = conn.cursor()

# # content 컬럼에서 bytea 타입의 값을 읽어오는 SQL 쿼리
# cur.execute("SELECT content FROM your_table WHERE id = %s", ("111",))

# # 결과 가져오기
# bytea_data = cur.fetchone()[0]

# # bytea 데이터를 UTF-8로 디코딩
# decoded_data = bytea_data.decode('utf-8')

# # 디코딩된 데이터를 JSON으로 변환
# json_data = json.loads(decoded_data)

# # JSON 데이터 출력
# print(json_data)

# # 커서와 연결 닫기
# cur.close()
# conn.close()

# row = json.loads(decode)