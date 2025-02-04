import csv
import tempfile
from pathlib import Path
from common_postgres_methods import CommonPostgresMethods
from sqlalchemy import text

class ConfluenceDedup:
    def __init__(self, postgres_conn_id):
        self.common_postgres_methods = CommonPostgresMethods(postgres_conn_id)

    def export_dsllm_raw_to_temp_csv_in_chunks(self, chunk_size):
        try:
            with tempfile.TemporaryDirectory() as tmpdirname:
                base_csv_file_path = Path(tmpdirname) / "dsllm_raw_chunk"
                self.common_postgres_methods.export_dsllm_raw_to_csv_in_chunks(base_csv_file_path, chunk_size)
                print(f"Data exported to temporary folder: {tmpdirname}")
        except Exception as e:
            print(f"Error exporting data in chunks: {e}")
            raise
    
    def export_dsllm_raw_to_temp_csv_in_chunks_sqlalchemy(self, chunk_size):
        try:
            with tempfile.TemporaryDirectory() as tmpdirname:
                base_csv_file_path = Path(tmpdirname) / "dsllm_raw_chunk"
                conn = self.common_postgres_methods.get_conn()
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM dsllm_raw")
                
                file_index = 1
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    
                    csv_file_path = base_csv_file_path.with_name(f"{base_csv_file_path.stem}_{file_index}.csv")
                    with open(csv_file_path, 'w') as f:
                        writer = csv.writer(f)
                        if file_index == 1:
                            writer.writerow([desc[0] for desc in cursor.description])  # write headers only once
                        writer.writerows(rows)
                    
                    print(f"Data chunk exported to {csv_file_path}")
                    file_index += 1
                
                cursor.close()
                conn.close()
                print(f"Data from dsllm_raw table exported to multiple CSV files with chunk size {chunk_size} using psycopg2")
        except Exception as e:
            print(f"Error exporting data in chunks using SQLAlchemy: {e}")
            raise
            
    def export_dsllm_raw_to_temp_csv_in_chunks_no_auto_delete(self, chunk_size):
        try:
            tmpdirname = Path(tempfile.mkdtemp())
            base_csv_file_path = tmpdirname / "dsllm_raw_chunk"
            self.common_postgres_methods.export_dsllm_raw_to_csv_in_chunks(base_csv_file_path, chunk_size)
            print(f"Data exported to temporary folder: {tmpdirname}")
        except Exception as e:
            print(f"Error exporting data in chunks without auto delete: {e}")
            raise

    def export_dsllm_raw_to_temp_csv_in_chunks_sqlalchemy_no_auto_delete(self, chunk_size):
        try:
            tmpdirname = Path(tempfile.mkdtemp())
            base_csv_file_path = tmpdirname / "dsllm_raw_chunk"
            conn = self.common_postgres_methods.get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM dsllm_raw")
            
            file_index = 1
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                
                csv_file_path = base_csv_file_path.with_name(f"{base_csv_file_path.stem}_{file_index}.csv")
                with open(csv_file_path, 'w') as f:
                    writer = csv.writer(f)
                    if file_index == 1:
                        writer.writerow([desc[0] for desc in cursor.description])  # write headers only once
                    writer.writerows(rows)
                
                print(f"Data chunk exported to {csv_file_path}")
                file_index += 1
            
            cursor.close()
            conn.close()
            print(f"Data from dsllm_raw table exported to multiple CSV files with chunk size {chunk_size} using psycopg2")
            print(f"Temporary folder: {tmpdirname}")
        except Exception as e:
            print(f"Error exporting data in chunks using SQLAlchemy without auto delete: {e}")
            raise

    def load_csv_to_temp_table(self, parent_folder, temp_table_name):
        try:
            conn = self.common_postgres_methods.get_conn()
            cursor = conn.cursor()
            temp_table_created = False
            for csv_file in Path(parent_folder).glob('*.csv'):
                with open(csv_file, 'r') as f:
                    reader = csv.reader(f)
                    headers = next(reader)
                    if not temp_table_created:
                        cursor.execute(f"CREATE TEMP TABLE {temp_table_name} ({', '.join([f'{header} TEXT' for header in headers])})")
                        temp_table_created = True
                    for row in reader:
                        cursor.execute(f"INSERT INTO {temp_table_name} VALUES ({', '.join(['%s' for _ in range(len(row))])})", row)
                print(f"Data from {csv_file} loaded into temporary table {temp_table_name}")
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            cursor.close()
            conn.close()
            print(f"Temporary table {temp_table_name} dropped")
        except Exception as e:
            print(f"Error loading CSV to temp table: {e}")
            raise

# Example usage:
dedup = ConfluenceDedup('your_postgres_conn_id')
# dedup.export_dsllm_raw_to_temp_csv_in_chunks(1000)
dedup.export_dsllm_raw_to_temp_csv_in_chunks_sqlalchemy(10)
# dedup.export_dsllm_raw_to_temp_csv_in_chunks_no_auto_delete(1000)
# dedup.export_dsllm_raw_to_temp_csv_in_chunks_sqlalchemy_no_auto_delete(1000)
dedup.load_csv_to_temp_table('/path/to/parent_folder', 'temp_table_name')
