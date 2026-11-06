import logging
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)

default_args = {
    'owner': 'me',
    'start_date': datetime(2026, 9, 11),
}

def log_start(**context):
    conf = context['dag_run'].conf or {}
    log.info("Second DAG has been triggered!")
    log.info(f"Received conf from parent DAG: {conf}")

with DAG(
    dag_id='second_dag',
    default_args=default_args,
    description='Запускается по триггеру из rate_pipeline',
    schedule=None,
    catchup=False,
    tags=['triggered'],
) as dag:

    t1 = PythonOperator(
        task_id='log_start',
        python_callable=log_start,
    )
