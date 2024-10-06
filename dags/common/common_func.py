import logging
import requests

def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    return logger

logger = setup_logger(__name__)

def send_email_via_rest_api(to_email, subject, body):
    url = "https://mail.a/mail/send"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer YOUR_API_KEY"
    }
    data = {
        "to": to_email,
        "subject": subject,
        "body": body
    }
    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
        logger.info(f"Email sent to {to_email} with status code {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending email: {e}")

def failure_callback(context):
    subject = f"DAG {context['dag'].dag_id} - Task {context['task_instance'].task_id} Failed"
    html_content = f"""
    <h3>DAG: {context['dag'].dag_id}</h3>
    <p>Task: {context['task_instance'].task_id} failed.</p>
    <p>Execution Date: {context['execution_date']}</p>
    """
    logger.error(f"Task {context['task_instance'].task_id} in DAG {context['dag'].dag_id} failed.")
    send_email_via_rest_api("your_email@example.com", subject, html_content)