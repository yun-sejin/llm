import requests
import logging
from airflow.utils.context import Context
from basenotifier import BaseNotifier

logger = logging.getLogger(__name__)

class EmailNotifier(BaseNotifier):
    def __init__(self, from_email: str, to: str, subject: str, api_endpoint: str):
        super().__init__()
        self.from_email = from_email
        self.to = to
        self.subject = subject
        self.api_endpoint = api_endpoint

    def notify(self, context: Context):
        subject = self.subject.format(
            dag_id=context['dag'].dag_id,
            task_id=context['task_instance'].task_id
        )
        message = {
            "subject": subject,
            "dag_id": context['dag'].dag_id,
            "task_id": context['task_instance'].task_id,
            "execution_date": str(context['execution_date']),
            "log_url": context['task_instance'].log_url
        }
        
        response = requests.post(self.api_endpoint, json=message)
        
        if response.status_code != 200:
            logger.error(f"Failed to send email via API. Status code: {response.status_code}, Response: {response.text}")
        else:
            logger.info("Email sent successfully via API.")