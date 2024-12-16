import json
from airflow.models import Variable

def get_dag_config(dag_id):
    """
    Retrieve the DAG configuration from Airflow Variables.
    
    :param dag_id: The ID of the DAG to retrieve the configuration for.
    :return: A dictionary containing the DAG configuration.
    """
    dag_info = json.loads(Variable.get("dag_info"))
    dag_config = next((item for item in dag_info if item["dag_id"] == dag_id), None)
    
    if dag_config is None:
        raise ValueError(f"DAG configuration not found for dag_id: {dag_id}")
    
    return dag_config