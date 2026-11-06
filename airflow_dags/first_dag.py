import json
import re
import email
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.imap.hooks.imap import ImapHook

default_args = {
    'owner': 'me',
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5),
    'start_date': datetime(2026, 9, 11),
}

def fetch_rate(**context):
    url = "https://money.robolatoriya.com/api/rate/current"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    result = {
        "time": datetime.now().isoformat(),
        "rate": data.get("rate"),
        "date_from_api": data.get("date"),
    }
    print(f"Fetched: {result}")
    return result

def read_rate_from_email(**context):
    with ImapHook(imap_conn_id='imap_default') as hook:
        mail_client = hook.mail_client

        mail_client.select('INBOX')

        status, messages = mail_client.search(None, 'FROM', 'lesha.chekalov@gmail.com')
        if status != 'OK' or not messages[0]:
            raise ValueError("No emails found from sender")

        latest_email_id = messages[0].split()[-1]

        status, msg_data = mail_client.fetch(latest_email_id, '(RFC822)')
        if status != 'OK':
            raise ValueError(f"Failed to fetch email: {status}")

        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email)

        subject = msg['Subject']
        print(f"Email subject: {subject}")

        match = re.search(r'Rate:\s*(\d+)', subject)
        if not match:
            raise ValueError(f"Could not parse rate from subject: {subject}")
        rate = int(match.group(1))
        print(f"Extracted rate: {rate}")

        return rate

def print_rate(**context):
    rate = context['ti'].xcom_pull(task_ids='read_rate_from_email')
    print(f"KEY RATE = {rate}")
    return rate

with DAG(
    dag_id='rate_pipeline',
    default_args=default_args,
    description='Пайплайн: API -> Email -> IMAP -> Print',
    schedule='*/5 * * * *',
    catchup=False,
    tags=['rate', 'email'],
) as dag:

    t1 = PythonOperator(
        task_id='fetch_rate',
        python_callable=fetch_rate,
    )

    t2 = EmailOperator(
        task_id='send_email',
        to='lesha.chekalov0202@gmail.com',
        subject='Rate: {{ ti.xcom_pull(task_ids="fetch_rate")["rate"] }} @ {{ ti.xcom_pull(task_ids="fetch_rate")["time"] }}',
        html_content='<p>Time: {{ ti.xcom_pull(task_ids="fetch_rate")["time"] }}</p>'
                     '<p>Rate: {{ ti.xcom_pull(task_ids="fetch_rate")["rate"] }}</p>',
    )

    t3 = PythonOperator(
        task_id='read_rate_from_email',
        python_callable=read_rate_from_email,
    )

    t4 = PythonOperator(
        task_id='print_rate',
        python_callable=print_rate,
    )

    t5 = TriggerDagRunOperator(
        task_id='trigger_second_dag',
        trigger_dag_id='second_dag',
        wait_for_completion=False,
        conf={'source_dag': 'rate_pipeline', 'rate': '{{ ti.xcom_pull(task_ids="read_rate_from_email") }}'},
    )

    t1 >> t2 >> t3 >> t4 >> t5
