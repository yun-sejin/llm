import psycopg2
import csv
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook

class CommonPostgresMethods(BaseHook):
    def __init__(self, postgres_conn_id):
        super().__init__()
        self.postgres_conn_id = postgres_conn_id
        # conn = self.get_connection(self.postgres_conn_id)
        # self.dbname = conn.schema
        # self.user = conn.login
        # self.password = conn.password
        # self.host = conn.host
        # self.port = conn.port
        # conn = self.get_connection(self.postgres_conn_id)
        self.dbname = 'llmdp',
        self.user = 'airflow',
        self.password = 'airflow',
        self.host = 'localhost',
        self.port = '5432'

    def get_conn(self):
        try:
            return psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        except psycopg2.Error as e:
            self.log.error(f"Database connection failed: {e}")
            raise

    def get_sqlalchemy_conn(self):
        try:
            connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname[0]}"
            engine = create_engine(connection_string)
            return engine.connect()
        except Exception as e:
            self.log.error(f"SQLAlchemy connection failed: {e}")
            raise

    def execute_sql(self, sql):
        try:
            conn = self.get_conn()
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            conn.close()
        except psycopg2.Error as e:
            self.log.error(f"SQL execution failed: {e}")
            raise

    def bulk_load_with_delimiter(self, table, tmp_file, delimiter=',', header=True):
        try:
            conn = self.get_conn()
            cursor = conn.cursor()
            
            header_option = "CSV HEADER" if header else "CSV"
            
            copy_sql = f"""
            COPY {table} FROM STDIN WITH
            {header_option}
            DELIMITER AS '{delimiter}'
            """
            
            with open(tmp_file, 'r') as f:
                cursor.copy_expert(sql=copy_sql, file=f)
            
            conn.commit()
            cursor.close()
            conn.close()
            self.log.info(f"Data from {tmp_file} loaded into {table} table with delimiter '{delimiter}' and header={header}")
        except psycopg2.Error as e:
            self.log.error(f"Bulk load failed: {e}")
            raise

    def insert_into_dsllm_raw(self, column1_data, column2_data):
        try:
            sql = """
            INSERT INTO dsllm_raw (column1, column2)
            VALUES (%s, %s)
            """
            conn = self.get_conn()
            cursor = conn.cursor()
            cursor.execute(sql, (column1_data, column2_data))
            conn.commit()
            cursor.close()
            conn.close()
            self.log.info(f"Data inserted into dsllm_raw: column1={column1_data}, column2={column2_data}")
        except psycopg2.Error as e:
            self.log.error(f"Insert into dsllm_raw failed: {e}")
            raise

    def export_dsllm_raw_to_csv(self, csv_file_path):
        try:
            sql = "SELECT * FROM dsllm_raw"
            conn = self.get_conn()
            cursor = conn.cursor()
            cursor.execute(sql)
            
            with open(csv_file_path, 'w') as f:
                writer = csv.writer(f)
                writer.writerow([desc[0] for desc in cursor.description])  # write headers
                for row in cursor.fetchall():
                    writer.writerow(row)
            
            cursor.close()
            conn.close()
            self.log.info(f"Data from dsllm_raw table exported to {csv_file_path}")
        except psycopg2.Error as e:
            self.log.error(f"Export to CSV failed: {e}")
            raise

    def export_dsllm_raw_to_csv_in_chunks(self, base_csv_file_path, chunk_size):
        try:
            sql = "SELECT * FROM dsllm_raw"
            conn = self.get_conn()
            cursor = conn.cursor()
            cursor.execute(sql)
            
            file_index = 1
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                
                csv_file_path = f"{base_csv_file_path}_{file_index}.csv"
                with open(csv_file_path, 'w') as f:
                    writer = csv.writer(f)
                    if file_index == 1:
                        writer.writerow([desc[0] for desc in cursor.description])  # write headers only once
                    writer.writerows(rows)
                
                self.log.info(f"Data chunk exported to {csv_file_path}")
                file_index += 1
            
            cursor.close()
            conn.close()
            self.log.info(f"Data from dsllm_raw table exported to multiple CSV files with chunk size {chunk_size}")
        except psycopg2.Error as e:
            self.log.error(f"Export to CSV in chunks failed: {e}")
            raise


