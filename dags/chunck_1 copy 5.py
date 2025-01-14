# 아래 예시는 data_list의 각 행에서 data 키의 값이 대용량일 때, data만 청크 단위로 나누어 PostgreSQL에 삽입하는 방식으로 수정한 코드입니다. 각 데이터의 크기를 바이트 단위로 계산하고 출력한 후, 이를 청크 단위로 PostgreSQL에 삽입합니다.

import psycopg2
from psycopg2 import Binary
import json

# PostgreSQL 연결 정보
connection_params = {
    'dbname': 'your_db_name',
    'user': 'your_db_user',
    'password': 'your_db_password',
    'host': 'your_db_host',
    'port': 'your_db_port'
}

# 고정된 데이터 리스트 (여섯 개 이상의 컬럼, address는 JSON 타입)
data_list = [
    {'src_id': '111', 'data': b'1212121' * 1000000, 'name': 'example', 'age': 30, 'email': 'example@example.com', 'address': json.dumps({'street': '123 Example St', 'city': 'Example City', 'zip': '12345'})},
    {'src_id': '112', 'data': b'1212121' * 1000000, 'name': 'example', 'age': 30, 'email': 'example@example.com', 'address': json.dumps({'street': '123 Example St', 'city': 'Example City', 'zip': '12345'})},
    {'src_id': '113', 'data': b'1212121' * 1000000, 'name': 'example', 'age': 30, 'email': 'example@example.com', 'address': json.dumps({'street': '123 Example St', 'city': 'Example City', 'zip': '12345'})},
    {'src_id': '114', 'data': b'1212121' * 1000000, 'name': 'example', 'age': 30, 'email': 'example@example.com', 'address': json.dumps({'street': '123 Example St', 'city': 'Example City', 'zip': '12345'})}
] * 25  # 예시로 100개의 레코드 생성

def print_data_sizes(data_list):
    """
    데이터 리스트의 각 데이터 크기를 바이트 단위로 출력하고,
    전체 data_list의 크기를 바이트 단위로 계산하여 출력합니다.
    """
    total_size = 0
    for i, record in enumerate(data_list):
        src_id_size = len(record['src_id'].encode('utf-8'))
        data_size = len(record['data'])
        name_size = len(record['name'].encode('utf-8'))
        age_size = len(str(record['age']).encode('utf-8'))
        email_size = len(record['email'].encode('utf-8'))
        address_size = len(record['address'].encode('utf-8'))
        record_size = src_id_size + data_size + name_size + age_size + email_size + address_size
        total_size += record_size
        print(f"Record {i}: src_id={record['src_id']}, src_id size={src_id_size} bytes, data size={data_size} bytes, name size={name_size} bytes, age size={age_size} bytes, email size={email_size} bytes, address size={address_size} bytes, total size={record_size} bytes")
    print(f"Total data size: {total_size} bytes")

def insert_data_in_chunks(data_list, chunk_size=1024 * 1024):  # 1MB 청크
    """
    데이터 리스트를 청크 단위로 PostgreSQL에 삽입합니다.
    """
    # conn = psycopg2.connect(**connection_params)
    # cur = conn.cursor()

    # # 테스트용 테이블 생성
    # cur.execute("DROP TABLE IF EXISTS test_data_chunks;")
    # cur.execute("""
    #     CREATE TABLE test_data_chunks (
    #         id SERIAL PRIMARY KEY,
    #         src_id TEXT,
    #         data BYTEA,
    #         name TEXT,
    #         age INT,
    #         email TEXT,
    #         address JSONB
    #     );
    # """)
    # conn.commit()

    print(f"Inserting data in chunks of {chunk_size} bytes...")

    for record in data_list:
        src_id = record['src_id']
        name = record['name']
        age = record['age']
        email = record['email']
        address = record['address']
        data = record['data']

        # 데이터 청크 단위로 나누어 삽입
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            print('chunk:', chunk)
    #         cur.execute(
    #             "INSERT INTO test_data_chunks (src_id, data, name, age, email, address) VALUES (%s, %s, %s, %s, %s, %s::jsonb)",
    #             (src_id, Binary(chunk), name, age, email, address)
    #         )
    #     conn.commit()

    # cur.close()
    # conn.close()
    print("Finished inserting data into test_data_chunks.")

if __name__ == "__main__":
    # 데이터 크기 출력
    print_data_sizes(data_list)

    # 고정된 데이터 리스트를 청크 단위로 삽입
    insert_data_in_chunks(data_list)
    
    
    
    
#     설명
# 고정된 데이터 리스트(data_list):

# 각 데이터는 여섯 개 이상의 컬럼을 가집니다: {'src_id': '111', 'data': b'1212121' * 1000000, 'name': 'example', 'age': 30, 'email': 'example@example.com', 'address': json.dumps({'street': '123 Example St', 'city': 'Example City', 'zip': '12345'})}.
# 예시로 100개의 레코드를 생성합니다.
# print_data_sizes(data_list):

# 데이터 리스트의 각 데이터 크기를 바이트 단위로 출력합니다.
# 각 레코드의 src_id, data, name, age, email, address 크기를 계산하여 출력합니다.
# 전체 data_list의 크기를 바이트 단위로 계산하여 출력합니다.
# insert_data_in_chunks(data_list, chunk_size=1024 * 1024):

# 데이터 리스트를 청크 단위로 PostgreSQL에 삽입합니다.
# chunk_size는 한 번에 삽입할 데이터의 바이트 수(기본 1MB)입니다.
# 데이터베이스 연결 후, 테이블을 생성하고, 각 레코드의 data를 청크 단위로 나누어 삽입합니다.
# 실행 순서:

# 데이터 크기를 출력합니다.
# 고정된 데이터 리스트를 청크 단위로 삽입합니다.
# 이 코드는 고정된 데이터 리스트의 각 데이터 크기를 바이트 단위로 확인하고, 전체 data_list의 크기를 계산한 후, 이를 청크 단위로 PostgreSQL에 삽입하는 방법을 보여줍니다. 이를 통해 데이터 크기를 확인하고, 메모리 사용량을 줄이며 "invalid memory alloc" 에러를 방지할 수 있습니다.