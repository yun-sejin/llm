from datetime import datetime
import json
import uuid
#import psycopg2

#파일 경로 
#C:\upload\20240626\aaa
    #> qna.json, qna3.json
#C:\upload\20240626\bbb
    #qna2.json, qna3.json

def upsert_all_data():
    base_dir = r'C:\upload\{}'.format(datetime.now().strftime('%Y%m%d'))
    base_dir2 = 'C:/upload/'
    # conn_details = BaseHook.get_connection('my_postgres_conn').get_uri()
    
    # conn = psycopg2.connect(conn_details)
    # cur = conn.cursor()
    
    # Fetch file_list from the dsllm_job_hist table
    #cur = conn.cursor()

    # SQL query to select job_id and file_list where job_type is 'upload' and success_yn is 'Y'
    # query = """
    # SELECT job_id, file_list FROM dsllm_job_hist
    # WHERE job_type = 'upload' AND success_yn = 'Y'
    # """
    # cur.execute(query)

    # # Fetch all matching rows
    # rows = cur.fetchall()

    # for row in rows:
    #     print(row)  # Each 'row' is a tuple with (job_id, file_list)

    #file_list = [['20240626/aaa/qna.json','20240626/aaa/qna3.json'], ['20240626/bbb/qna2.json','20240626/bbb/qna3.json']]
    file_list = (('123345', ['20240626/aaa/qna.json','20240626/aaa/qna3.json']), ('765432',['20240626/bbb/qna2.json','20240626/bbb/qna3.json']))
    rows = (('123345', ['20240626/aaa/qna.json','20240626/aaa/qna3.json']), ('765432',['20240626/bbb/qna2.json','20240626/bbb/qna3.json']))
    # for sub_list in file_list:
    for row in rows:
        print(row)
        file_list0 = row[0]
        file_list = row[1]
        all_data = []
        for file_path in file_list:
            with open(f"{base_dir2}/{file_path}", 'r') as f:
                data = json.load(f)
                all_data.append(data)
        print(all_data)        
        #all_data = [{'id': '345', 'job_id': '12345', 'sub_id': '5678', 'all': {'question': 'What is the capital of France?', 'answer': 'seoul'}}, {'id': '345', 'job_id': '12345', 'sub_id': '5678', 'all': {'question': 'What is the capital of France?', 'answer': 'seoul'}}]
        data_list = ()
        for data in all_data:
            uuid_id = uuid.uuid4()
            id = data["id"]
            job_id = data["job_id"]
            sub_id = data["sub_id"]
            data_tuple = (id, job_id, sub_id)
            data_list.append((str(uuid_id), json.dumps(data_tuple)))
        print("data_list:",data_list)
        #결과값 data_list = [('b3e5b8e1-2f7b-4c0f-9f8d-5c3b6d2c9b1d', '{"id": "345", "job_id": "12345", "sub_id": "5678"}'), ('b3e5b8e1-2f7b-4c0f-9f8d-5c3b6d2c9b1d', '{"id": "345", "job_id": "12345", "sub_id": "5678"}')]
        
        query = "INSERT INTO dsllm_job_hist (uuid, data) VALUES (%s, %s)"
        psycopg2.extras.execute_batch(cur, query, data_list)

        query = "INSERT INTO dsllm_raw (uuid, data) VALUES (%s, %s)"
        psycopg2.extras.execute_batch(cur, query, data_list)
          

    # print(all_data)
    # print('#'*20)
    # print(f"Loaded {len(all_data)} JSON files.")

    # conn.commit()
    # conn.close()

upsert_all_data()