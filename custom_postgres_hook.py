"""
Custom Postgres Hook for Airflow
"""

from sqlalchemy import create_engine
import pandas as pd

from common_postgres_methods import CommonPostgresMethods

class CustomPostgresHook(CommonPostgresMethods):
    def __init__(self, postgres_conn_id):
        super().__init__(postgres_conn_id)
        self.user = 'your_user'
        self.password = 'your_password'
        self.dbname = 'your_dbname'
        
    def bulk_load_with_delimiter(self, table, tmp_file, delimiter=',', header=True, is_replace=True):
        df = pd.read_csv(tmp_file, delimiter=delimiter, header=0 if header else None)
        uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
        self.engine = create_engine(uri)

    def bulk_load_with_pandas(self, table, tmp_file, delimiter=',', header=True, is_replace='append'):
        engine = self.engine
        df = pd.read_csv(tmp_file, delimiter=delimiter, header=0 if header else None)
        if_exists = 'replace' if is_replace else 'append'
        df = df.applymap(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)  # Remove newline characters from all columns
        df.to_sql(table, engine, if_exists=if_exists, index=False)
        
    def create_temp_table(self, table_name):
        create_table_sql = f"""
        CREATE TEMP TABLE {table_name} (
            src_type TEXT,
            title TEXT,
            description TEXT,
            data TEXT,
            create_time TIMESTAMP
        );
        """
        self.execute_sql(create_table_sql)
        self.log.info(f"Temporary table {table_name} created")

    def drop_temp_table(self, table_name):
        drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
        self.execute_sql(drop_table_sql)
        self.log.info(f"Temporary table {table_name} dropped")
