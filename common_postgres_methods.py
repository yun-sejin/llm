import psycopg2
from airflow.hooks.base import BaseHook

class CommonPostgresMethods(BaseHook):
    def __init__(self, postgres_conn_id):
        super().__init__()
        self.postgres_conn_id = postgres_conn_id
        conn = self.get_connection(self.postgres_conn_id)
        self.dbname = conn.schema
        self.user = conn.login
        self.password = conn.password
        self.host = conn.host
        self.port = conn.port

    def get_conn(self):
        return psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    def execute_sql(self, sql):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

    def bulk_load_with_delimiter(self, table, tmp_file, delimiter=',', header=True):
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
