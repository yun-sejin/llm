import psycopg2
from psycopg2.extras import execute_values

def insert_with_execute_values(data, table_name):
    try:
        conn = psycopg2.connect(
            dbname='llmdp',
            user='airflow',
            password='airflow',
            host='localhost',
            port='5432'
        )
        cursor = conn.cursor()
        
        sql = f"INSERT INTO {table_name} (column1, column2) VALUES %s"
        execute_values(cursor, sql, data)
        total_inserted = cursor.rowcount
        print(f"Total rows to be inserted: {total_inserted}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"Total rows inserted: {total_inserted}")
        return total_inserted
    
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        raise

def create_temp_table_and_insert(data, source_table, temp_table):
    try:
        conn = psycopg2.connect(
            dbname='llmdp',
            user='airflow',
            password='airflow',
            host='localhost',
            port='5432'
        )
        cursor = conn.cursor()
        # Create a temporary table with the same structure as source_table
        create_sql = f"CREATE TEMP TABLE {temp_table} AS SELECT * FROM {source_table} WHERE 1=0"
        cursor.execute(create_sql)
        # Insert data into the temp table using execute_values
        insert_sql = f"INSERT INTO {temp_table} (column1, column2) VALUES %s"
        execute_values(cursor, insert_sql, data)
        total_inserted = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Total rows inserted into temp table {temp_table}: {total_inserted}")
        return total_inserted
    except psycopg2.Error as e:
        print(f"Error inserting data into temp table: {e}")
        raise

def select_with_fetchmany(data, table_name, chunk_size):
    try:
        # Use context manager to ensure connection cleanup and temp table dropping
        with psycopg2.connect(
            dbname='llmdp',
            user='airflow',
            password='airflow',
            host='localhost',
            port='5432'
        ) as conn:
            # Create temp table using a dedicated cursor
            with conn.cursor() as cur:
                create_sql = f"CREATE TEMP TABLE temp_table AS SELECT * FROM {table_name} WHERE 1=0"
                cur.execute(create_sql)
            
            # Execute select query on source table with a separate cursor
            with conn.cursor() as cursor:
                sql = f"SELECT * FROM {table_name}"
                cursor.execute(sql)
                
                total_selected = 0
                total_inserted_all = 0
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    total_selected += len(rows)
                    
                    # Use separate cursor for modifying temp table per chunk
                    with conn.cursor() as cur:
                        cur.execute("TRUNCATE TABLE temp_table")
                        insert_sql = f"INSERT INTO temp_table (column1, column2) VALUES %s"
                        execute_values(cur, insert_sql, rows)
                        inserted = cur.rowcount
                        total_inserted_all += inserted
                        
                        # Insert data from temp_table into dsllm_deduped table.
                        copy_sql = "INSERT INTO dsllm_deduped (column1, column2) SELECT column1, column2 FROM temp_table"
                        cur.execute(copy_sql)
                                       
                conn.commit()
                
                print(f"Total rows selected: {total_selected}")
                print(f"Overall total rows inserted into temp table: {total_inserted_all}")
                
                return total_inserted_all
    
    except psycopg2.Error as e:
        print(f"Error selecting data: {e}")
        raise

if __name__ == "__main__":
    data = [
        ('1', '111'),
        ('2', '222'),
        ('3', '333')
    ]
    table_name = 'dsllm_raw'
    
    # chunk_size = 5000  # Optimal chunk size for processing over 50,000 rows
    # chunk_size = 1000  # Optimal chunk size for approximately 50,000 rows  
    # chunk_size = 10000  # Optimized chunk size for 50,000 ~ 60,000 rows
    chunk_size = 7
    total_selected = select_with_fetchmany(data,table_name, chunk_size)
    print(f"Total rows selected: {total_selected}")