#아래 코드는 대량 데이터를 Postgres에 삽입하기 전에 작은 용량으로 테스트해볼 수 있는 예제입니다. 이 예제에서는 10MB 정도의 HTML 데이터를 생성한 뒤, 청크(Chunk)로 나누어 PostgreSQL 테이블에 삽입하는 과정을 시뮬레이션합니다.

import os
import psycopg2
from psycopg2 import sql, Binary

# 테스트용으로 약 10MB 정도의 HTML 데이터를 생성하는 함수
def generate_test_html_file(output_file, total_size=10 * 1024 * 1024):
    chunk_text = (
        "<p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
        "Proin facilisis lectus in neque bibendum, at cursus metus dictum. "
        "Vestibulum non metus aliquam, porttitor augue ut, consectetur sem.</p>\n"
    )
    chunk_size = len(chunk_text.encode('utf-8'))
    repeat_count = total_size // chunk_size
    remainder = total_size % chunk_size

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("<html><body>\n")
        for _ in range(repeat_count):
            f.write(chunk_text)
        if remainder > 0:
            f.write(chunk_text[:remainder])
        f.write("</body></html>\n")

# PostgreSQL 연결 정보 (테스트 환경에 맞게 수정)
connection_params = {
    'dbname': 'your_db_name',
    'user': 'your_db_user',
    'password': 'your_db_password',
    'host': 'your_db_host',
    'port': 'your_db_port'
}

# HTML 파일을 청크로 나누어 INSERT
def insert_html_in_chunks(html_file, chunk_size=1024*1024):  # 기본 1MB씩 청크
    conn = psycopg2.connect(**connection_params)
    cur = conn.cursor()

    # 테스트용 테이블 생성
    cur.execute("DROP TABLE IF EXISTS test_html_chunks;")
    cur.execute("""
        CREATE TABLE test_html_chunks (
            id SERIAL PRIMARY KEY,
            chunk_no INT,
            data BYTEA
        );
    """)
    conn.commit()

    print(f"Reading {html_file} in chunks of {chunk_size} bytes...")

    with open(html_file, 'rb') as f:
        chunk_no = 0
        while True:
            chunk_data = f.read(chunk_size)
            if not chunk_data:
                break
            chunk_no += 1
            cur.execute(
                "INSERT INTO test_html_chunks (chunk_no, data) VALUES (%s, %s)",
                (chunk_no, Binary(chunk_data))
            )
            # 필요에 따라 성능 테스트 시에는 batch commit 가능
            conn.commit()

    cur.close()
    conn.close()
    print("Finished inserting chunks into test_html_chunks.")

if __name__ == "__main__":
    # 1) 테스트용 HTML 파일 생성
    test_html_path = "test_10MB.html"
    generate_test_html_file(test_html_path, total_size=10 * 1024 * 1024)
    print(f"Test HTML file created: {test_html_path}")

    # 2) 생성된 파일을 청크 단위로 삽입
    insert_html_in_chunks(test_html_path)
    
    
#  설명:

# generate_test_html_file 함수

# 약 10MB 정도의 HTML 파일을 생성합니다(크기는 매개변수 total_size로 조절 가능).
# chunk_text라는 짧은 HTML을 여러 번 반복 작성합니다.
# insert_html_in_chunks 함수

# 생성된 HTML 파일을 열어, chunk_size 바이트씩 읽어 PostgreSQL 테이블에 여러 로우로 저장합니다.
# 한 줄에 큰 데이터가 들어가지 않도록 분산시켜 메모리 사용량을 줄이고, “invalid memory alloc” 에러를 방지합니다.
# 실행 순서
# a) 10MB HTML 파일 생성
# b) 생성된 파일을 읽어 1MB 단위로 잘라 INSERT

# 이 테스트 예제를 통해 대용량 데이터를 나누어 삽입하는 방법을 실험할 수 있으며, 이를 바탕으로 100MB, 1GB 등 더 큰 파일로 확장하여 적용할 수 있습니다.   
    