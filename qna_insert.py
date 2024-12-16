import json

from llm.db_connection import create_db_connection

def read_json_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def upsert_data(data):
   
    # UPSERT SQL 명령어
    sql = """
    INSERT INTO dsllm_raw (id, job_id, sub_id, data)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
    job_id = EXCLUDED.job_id,
    sub_id = EXCLUDED.sub_id,
    data = EXCLUDED.data;
    """
    
    try:
        # 데이터베이스에 연결
        conn = create_db_connection()
        cur = conn.cursor()
        
        # 'data' 딕셔너리 전체를 JSON 문자열로 변환 후 바이트로 인코딩
        bytea_data = json.dumps(data).encode('utf-8')
        
        # UPSERT 명령어 실행
        cur.execute(sql, (data['id'], data['job_id'], data['sub_id'], bytea_data))
        
        # 트랜잭션 커밋
        conn.commit()
        
        print("Data upserted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # 커서와 연결 종료
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
