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
    {'src_id': '111', 'data': b'1212121', 'name': 'example', 'age': 30, 'email': 'example@example.com', 'address': json.dumps({'street': '123 Example St', 'city': 'Example City', 'zip': '12345'})},
    {'src_id': '112', 'data': b'1212121', 'name': 'example', 'age': 30, 'email': 'example@example.com', 'address': json.dumps({'street': '123 Example St', 'city': 'Example City', 'zip': '12345'})},
    {'src_id': '113', 'data': b'1212121', 'name': 'example', 'age': 30, 'email': 'example@example.com', 'address': json.dumps({'street': '123 Example St', 'city': 'Example City', 'zip': '12345'})},
    {'src_id': '114', 'data': b'1212121', 'name': 'example', 'age': 30, 'email': 'example@example.com', 'address': json.dumps({'street': '123 Example St', 'city': 'Example City', 'zip': '12345'})}
] * 100000  # 예시로 40만 개의 레코드 생성 (약 280MB)

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

def insert_data_in_chunks(data_list, chunk_size=100):
    """
    데이터 리스트를 청크 단위로 PostgreSQL에 삽입합니다.
    """
    conn = psycopg2.connect(**connection_params)
    cur = conn.cursor()

    # 테스트용 테이블 생성
    cur.execute("DROP TABLE IF EXISTS test_data_chunks;")
    cur.execute("""
        CREATE TABLE test_data_chunks (
            id SERIAL PRIMARY KEY,
            src_id TEXT,
            data BYTEA,
            name TEXT,
            age INT,
            email TEXT,
            address JSONB
        );
    """)
    conn.commit()

    print(f"Inserting data in chunks of {chunk_size} records...")

    for i in range(0, len(data_list), chunk_size):
        chunk = data_list[i:i + chunk_size]
        for record in chunk:
            cur.execute(
                "INSERT INTO test_data_chunks (src_id, data, name, age, email, address) VALUES (%s, %s, %s, %s, %s, %s::jsonb)",
                (record['src_id'], Binary(record['data']), record['name'], record['age'], record['email'], record['address'])
            )
        conn.commit()

    cur.close()
    conn.close()
    print("Finished inserting data into test_data_chunks.")

if __name__ == "__main__":
    # 데이터 크기 출력
    print_data_sizes(data_list)

    # 고정된 데이터 리스트를 청크 단위로 삽입
    insert_data_in_chunks(data_list)