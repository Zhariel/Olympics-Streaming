from airflow import DAG
from kafka import KafkaProducer
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task
from datetime import datetime
import os

producer = KafkaProducer(bootstrap_servers='0.0.0.0:9092', api_version=(0, 10, 2))


@task(task_id="print_the_context")
def print_context(ds=None, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    return 'Whatever you return gets printed in the logs'

run_this = print_context()