from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import OperationalError
import yaml

# 데이터베이스 연결 함수
# config.yaml 파일을 사용하여 연결
def create_db_connection():
    try:
        # 데이터베이스 연결 설정
        connection = psycopg2.connect(
            host="localhost",
            database="your_database_name",
            user="your_username",
            password="your_password"
        )
        print("Database connection successful")
        return connection
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        return None
    

    
# 데이터베이스 연결 함수
# airflow웹의 admin에서 variable로 설정한 값으로 연결
def create_db_connection2():
    try:
        connection = psycopg2.connect(
            host=Variable.get("db_host"),
            database=Variable.get("db_database"),
            user=Variable.get("db_user"),
            password=Variable.get("db_password")
        )
        print("Database connection successful")
        return connection
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        return None



# YAML 파일에서 설정 읽어오는 함수
def read_config():
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    return config

# 데이터베이스 연결 함수
def create_db_connection():
    config = read_config()
    db_config = config['database']  # YAML 파일 구조에 맞게 경로 수정
    
    try:
        connection = psycopg2.connect(
            host=db_config['host'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        print("Database connection successful")
        return connection
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        return None
    