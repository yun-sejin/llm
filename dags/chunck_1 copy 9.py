# 예시 주의: 실제 실행 시 약 1GB의 메모리를 사용하므로,
# 충분한 메모리와 시간, 디스크 공간이 필요합니다.

import os
import psycopg2
from psycopg2 import sql, Binary

# PostgreSQL 연결 정보
connection_params = {
    'dbname': 'your_db_name',
    'user': 'your_db_user',
    'password': 'your_db_password',
    'host': 'your_db_host',
    'port': 'your_db_port'
}

def insert_1gb_data():
    # 1GB에 해당하는 랜덤 바이트 생성 (1024 * 1024 * 1024 = 1GB)
    print("Generating 1GB of random data in memory...")
    one_gb_data = os.urandom(1024 * 1024 * 1024)
    print("Data generation complete.")

    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**connection_params)
    cur = conn.cursor()

    # 테스트용 테이블 생성
    print("Creating test table...")
    cur.execute("DROP TABLE IF EXISTS test_large_bytea;")
    cur.execute("CREATE TABLE test_large_bytea (id SERIAL PRIMARY KEY, data BYTEA);")

    # 1GB 데이터를 INSERT
    print("Inserting 1GB data into test_large_bytea table. This may take a while...")
    insert_query = "INSERT INTO test_large_bytea (data) VALUES (%s);"
    cur.execute(insert_query, (Binary(one_gb_data),))

    # 커밋 후 리소스 정리
    conn.commit()
    cur.close()
    conn.close()
    print("Insertion complete and connection closed.")

if __name__ == "__main__":
    insert_1gb_data()