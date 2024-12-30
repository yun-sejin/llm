import psycopg2
from psycopg2.extras import Json

def insert_error_log(message, connection_params):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        # Insert each message into the failed_files table
        for item in message:
            cursor.execute(
                """
                INSERT INTO failed_files (src_id, error, etc)
                VALUES (%s, %s, %s)
                """,
                (item['src_id'], item['error'], Json(item['etc']))
            )
        
        # Commit the transaction
        conn.commit()
        print("Messages inserted successfully.")
          
    except Exception as e:
        print(f"Failed to insert messages: {e}")
        
    finally:
        # Close the database connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def update_email_sent_status(src_id, connection_params):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        # Update the email_sent status to True
        cursor.execute(
            """
            UPDATE failed_files
            SET email_sent = TRUE
            WHERE src_id = %s
            """,
            (src_id,)
        )
        
        # Commit the transaction
        conn.commit()
        print(f"Email sent status updated for src_id: {src_id}")
        
    except Exception as e:
        print(f"Failed to update email sent status: {e}")
        
    finally:
        # Close the database connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Example usage
message = [{
    "src_id": "qna_dsqna_1.",
    "error": "Another error occurred1.",
    "etc": [{"a":"Another error occurred1." , "b":"Another error occurred1."}]
    },{
    "src_id": "qna_dsqna_2.",
    "error": "Another error occurred2.",
    "etc": [{"a":"Another error occurred1." , "b":"Another error occurred1."}]
    },{
    "src_id": "qna_dsqna_3.",
    "error": "Another error occurred3.",
    "etc": [{"a":"Another error occurred1." , "b":"Another error occurred1."}]
    }]

connection_params = {
    'dbname': 'your_db_name',
    'user': 'your_db_user',
    'password': 'your_db_password',
    'host': 'your_db_host',
    'port': 'your_db_port'
}

insert_error_log(message, connection_params)

# Example of updating email sent status
update_email_sent_status("qna_dsqna_1.", connection_params)
