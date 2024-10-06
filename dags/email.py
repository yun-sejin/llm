from __future__ import annotations

import pendulum
import logging
import pdb
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email
import requests


def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # 콘솔 핸들러와 포맷터 설정
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # 핸들러를 로거에 추가
    logger.addHandler(console_handler)
    return logger


# 로거 설정 함수 호출
logger = setup_logger(__name__)

def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # 콘솔 핸들러와 포맷터 설정
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # 핸들러를 로거에 추가
    logger.addHandler(console_handler)
    return logger

logger = setup_logger(__name__)

def failure_callback(context):
    subject = f"DAG {context['dag'].dag_id} - Task {context['task_instance'].task_id} Failed"
    message = {
        "subject": subject,
        "dag_id": context['dag'].dag_id,
        "task_id": context['task_instance'].task_id,
        "execution_date": str(context['execution_date']),
        "log_url": context['task_instance'].log_url
    }
    
    response = requests.post('http://your-api-endpoint.com/send-email', json=message)
    
    if response.status_code != 200:
        logger.error(f"Failed to send email via API. Status code: {response.status_code}, Response: {response.text}")
    else:
        logger.info("Email sent successfully via API.")

@dag(
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        'email_on_failure': True,
        'email': ['luemier@daum.net'],
        'on_failure_callback': failure_callback
    }
)
def email():
    @task
    def first_task():
        logger.info("This is the first task.")
        # pdb.set_trace()  # 디버거 설정
    
    @task
    def end_task():
        logger.info("This is the SECOND task.")
        breakpoint()  # 디버거 설정
        # Simulate task failure for demonstration purposes
        # raise ValueError("This is a simulated task failure.")

    first_task() >> end_task()

DAG = email()

if __name__ == 'main':
    DAG.test()
    