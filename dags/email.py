from __future__ import annotations

import pendulum
import logging
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email


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

def failure_callback(context):
    subject = f"DAG {context['dag'].dag_id} - Task {context['task_instance'].task_id} Failed"
    html_content = f"""
    <h3>DAG: {context['dag'].dag_id}</h3>
    <p>Task: {context['task_instance'].task_id} failed.</p>
    <p>Execution Date: {context['execution_date']}</p>
    <p>Log URL: <a href="{context['task_instance'].log_url}">Log</a></p>
    """
    send_email('luemier@daum.net', subject, html_content)

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
        logger.error("This is the first task.")
    
    @task
    def end_task():
         logger.error("This is the SECOND task.")
        # Simulate task failure for demonstration purposes
        # raise ValueError("This is a simulated task failure.")

    first_task() >> end_task()

dag_instance = email()