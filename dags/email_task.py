from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.utils.email import send_email  # Import send_email

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
)
def email_task():
    @task
    def first_task():
        print("This is the first task.")

    @task(on_failure_callback=failure_callback)
    def end_task():
        # Simulate task failure for demonstration purposes
        raise ValueError("This is a simulated task failure.")

    first_task() >> end_task()

dag_instance = email_task()