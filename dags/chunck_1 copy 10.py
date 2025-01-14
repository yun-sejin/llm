import psycopg2
from psycopg2.extras import Json, execute_values
import json

def insert_llm_raw(data, connection_params):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        # Convert data to JSON string and then to bytes
        data_bytes = json.dumps(data['data']).encode('utf-8')
        
        # Insert data into the llm_raw table
        cursor.execute(
            """
            INSERT INTO llm_raw (src_id, src_type, src_sub_type, auth_group_lsit, data, createtime, wgt_param)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (data['src_id'], data['src_type'], data['src_sub_type'], Json(data['auth_group_lsit']), data_bytes, data['createtime'], Json(data['wgt_param']))
        )
        
        # Commit the transaction
        conn.commit()
        print("Data inserted successfully into llm_raw.")
        
    except Exception as e:
        print(f"Failed to insert data into llm_raw: {e}")
        
    finally:
        # Close the database connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def insert_llm_raw_batch(data_list, connection_params):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        # Prepare the data for batch insertion
        values = [
            (data['src_id'], data['src_type'], data['src_sub_type'], Json(data['auth_group_lsit']), json.dumps(data['data']).encode('utf-8'), data['createtime'], Json(data['wgt_param']))
            for data in data_list
        ]
        
        # Insert data into the llm_raw table using execute_values
        execute_values(
            cursor,
            """
            INSERT INTO llm_raw (src_id, src_type, src_sub_type, auth_group_lsit, data, createtime, wgt_param)
            VALUES %s
            """,
            values
        )
        
        # Commit the transaction
        conn.commit()
        print("Batch data inserted successfully into llm_raw.")
        
    except Exception as e:
        print(f"Failed to insert batch data into llm_raw: {e}")
        
    finally:
        # Close the database connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Example usage
data = {
    "src_id": "5f1b3b3b9f1f4b0001f3b3b3",
    "src_type": "conff",
    "src_sub_type": "dag",
    "auth_group_lsit": ["llm"],
    "data": {
        "name": "conff copy",
        "createid": "conff copy",
        "modifiedid": "llm",
        "modifiedtime": "20241218121230",
        "dag": {
            "tasks": [
                {
                    "task_id": "task_1",
                    "operator": "BashOperator",
                    "bash_command": "echo 'hello world'"
                }
            ]
        },
        "tags": ["example"],
        "add_info": {"space_id": "aaa", "space_name": "bbb"}
    },
    "createtime": "20241218121230",
    "wgt_param": {"count": 11, "view": 34}
}

# Generate maximum possible data for a single row
max_data = {
    "src_id": "5f1b3b3b9f1f4b0001f3b3b3",
    "src_type": "conff",
    "src_sub_type": "dag",
    "auth_group_lsit": ["llm"] * 1000,  # Example large list
    "data": {
        "name": "conff copy" * 1000,  # Example large string
        "createid": "conff copy" * 1000,
        "modifiedid": "llm" * 1000,
        "modifiedtime": "20241218121230",
        "dag": {
            "tasks": [
                {
                    "task_id": "task_1",
                    "operator": "BashOperator",
                    "bash_command": "echo 'hello world'" * 1000
                }
            ] * 1000  # Example large list of tasks
        },
        "tags": ["example"] * 1000,
        "add_info": {"space_id": "aaa" * 1000, "space_name": "bbb" * 1000}
    },
    "createtime": "20241218121230",
    "wgt_param": {"count": 11, "view": 34}
}

data_list = [data, max_data]  # Example list with normal and maximum data for batch insertion

connection_params = {
    'dbname': 'your_db_name',
    'user': 'your_db_user',
    'password': 'your_db_password',
    'host': 'your_db_host',
    'port': 'your_db_port'
}

insert_llm_raw(data, connection_params)
insert_llm_raw_batch(data_list, connection_params)
