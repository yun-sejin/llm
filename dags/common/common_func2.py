import logging
import requests
from airflow.hooks.base import BaseHook

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

class CustomEmailHook(BaseHook):
    def __init__(self, conn_id: str):
        super().__init__()
        self.conn_id = conn_id
        self.conn = self.get_connection(conn_id)

    def get_connection(self, conn_id: str):
        """
        Get the connection object from Airflow's connection management.
        """
        return BaseHook.get_connection(conn_id)

    def send_email(self, to_email, subject, body):
        url = self.conn.host
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.conn.password}"
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
    email_hook = CustomEmailHook(conn_id="email_api")
    email_hook.send_email("your_email@example.com", subject, html_content)