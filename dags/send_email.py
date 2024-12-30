import os
import requests
import psycopg2
from psycopg2.extras import Json
from jinja2 import Environment, FileSystemLoader

def create_html_content(message):
    # Load the Jinja template
    env = Environment(loader=FileSystemLoader(os.path.dirname(__file__)))
    template = env.get_template('template.html.jinja')
    
    # Render the template with the message content
    html_content = template.render(
        title='Error Report',
        heading='Error Details',
        rows=[{'cell1': item['src_id'], 'cell2': item['error'], 'cell3': item['etc']} for item in message]
    )
    
    return html_content

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

def send_email(subject, recipient, html_content, api_url, api_key):
    payload = {
        'subject': subject,
        'recipient': recipient,
        'html_content': html_content
    }
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    response = requests.post(api_url, json=payload, headers=headers)
    
    if response.status_code == 200:
        print(f'Email sent to {recipient}')
        return True
    else:
        print(f'Failed to send email: {response.status_code} - {response.text}')
        return False

def process_and_send_emails(connection_params, subject, recipient, api_url, api_key):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        # Query failed_files where email_sent is False
        cursor.execute(
            """
            SELECT src_id, error, etc
            FROM failed_files
            WHERE email_sent = FALSE
            """
        )
        rows = cursor.fetchall()
        
        # Prepare the message content
        message = [{'src_id': row[0], 'error': row[1], 'etc': row[2]} for row in rows]
        
        # Create the HTML content
        html_content = create_html_content(message)
        
        # Send the email
        if send_email(subject, recipient, html_content, api_url, api_key):
            # Update email_sent status only if email was sent successfully
            for row in rows:
                update_email_sent_status(row[0], connection_params)
        
    except Exception as e:
        print(f"Failed to process and send emails: {e}")
        
    finally:
        # Close the database connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Example usage
connection_params = {
    'dbname': 'your_db_name',
    'user': 'your_db_user',
    'password': 'your_db_password',
    'host': 'your_db_host',
    'port': 'your_db_port'
}

process_and_send_emails(
    connection_params=connection_params,
    subject='Error Report',
    recipient='recipient@example.com',
    api_url='https://api.example.com/send-email',
    api_key='your_api_key'
)
