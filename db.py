import json
import psycopg2
import uuid 

def upsert_data(data, conn):
    """
    Checks if data exists in the database based on id, and updates it if it does,
    otherwise inserts the new data with id, job_id, sub_id, and the entire data dictionary
    serialized and stored in a bytea column.

    Parameters:
    - data: The data to be inserted or updated, containing 'id', 'job_id', and 'sub_id' keys.
    - conn: Active database connection object.
    """
    cursor = conn.cursor()
    data_id = data['id']  # Extract the id from the data dictionary
    job_id = data['job_id']
    sub_id = data['sub_id']

    # Serialize data dictionary to JSON and encode to bytes
    data_bytes = json.dumps(data).encode('utf-8')

    # Check if the record exists based on id
    cursor.execute("SELECT id FROM dsllm_raw WHERE id = %s", (data_id,))
    exists = cursor.fetchone()
    
    if exists:
        # Record exists, update it
        update_command = """
        UPDATE dsllm_raw SET job_id = %s, sub_id = %s, data = %s WHERE id = %s;
        """
        cursor.execute(update_command, (job_id, sub_id, psycopg2.Binary(data_bytes), data_id))
    else:
        # Record does not exist, insert it
        insert_command = """
        INSERT INTO dsllm_raw (id, job_id, sub_id, data) VALUES (%s, %s, %s, %s);
        """
        cursor.execute(insert_command, (data_id, job_id, sub_id, psycopg2.Binary(data_bytes)))
    
    # Fetch the last job_id with success_yn as NULL
    cursor.execute("SELECT job_id FROM dsllm_job_hist WHERE success_yn IS NULL ORDER BY timestamp DESC LIMIT 1")
    last_job_id_with_null_success = cursor.fetchone()

    # If there's no such record, last_job_id_with_null_success will be None
    job_id = last_job_id_with_null_success[0] if last_job_id_with_null_success else None

    # Generate a UUID for the job_id
    job_uuid = uuid.uuid4()

    # Insert into dsllm_job_hist with the new job_uuid
    insert_command = """
    INSERT INTO dsllm_job_hist (job_id, success_yn, timestamp) VALUES (%s, %s, NOW());
    """
    # Assuming the operation was successful, hence True for success_yn
    cursor.execute(insert_command, (str(job_uuid), True))

    # Update dsllm_job_hist where job_id matches pre_job_id
    update_command = """
    UPDATE dsllm_job_hist SET success_yn = TRUE, post_job_id = %s WHERE job_id = %s;
    """
    cursor.execute(update_command, (str(job_uuid),job_id))

    conn.commit()
    cursor.close()