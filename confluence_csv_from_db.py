import csv
from common_postgres_methods import CommonPostgresMethods

class ConfluenceCSVExporter:
    def __init__(self, postgres_conn_id):
        self.common_postgres_methods = CommonPostgresMethods(postgres_conn_id)

    def export_table_to_csv_in_chunks(self, table_name, base_csv_file_path, chunk_size):
        try:
            sql = f"SELECT * FROM {table_name}"
            conn = self.common_postgres_methods.get_conn()
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
                
                print(f"Data chunk exported to {csv_file_path}")
                file_index += 1
            
            cursor.close()
            conn.close()
            print(f"Data from {table_name} table exported to multiple CSV files with chunk size {chunk_size}")
        
        except Exception as e:
            print(f"Error exporting table to CSV: {e}")
            raise
