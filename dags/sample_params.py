import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from common.common import get_dag_config  # Import the get_dag_config function

# Retrieve the configuration for the specific DAG
dag_config = get_dag_config("example_display_name")

# [START dag_decorator_usage]
@dag(
    dag_id=dag_config["dag_id"],
    schedule=dag_config["schedule"],
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=dag_config["tags"],
    dag_display_name="Sample DAG with Display Name",
)
def example_display_name():
    sample_task_1 = EmptyOperator(
        task_id="sample_task_1",
        task_display_name="Sample Task 1",
    )

    @task(task_display_name="Sample Task 2")
    def sample_task_2():
        pass

example_display_name()