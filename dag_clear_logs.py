import datetime as dt
import os
from airflow import DAG
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email_smtp
from textwrap import dedent
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

import warnings

warnings.filterwarnings("ignore")

date_today = dt.datetime.today()
dag_id = "clear_logs"
email_notification_list_failure = [
    "eroshevichdv@sber-bank.by",
]

email_notification_list_success = [
    "eroshevichdv@sber-bank.by",
]

args = {
    "owner": "BICC",
    "start_date": dt.datetime(2024, 8, 25),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=2),
    "depends_on_past": False,
    "email": email_notification_list_failure,
    "email_on_failure": True,
}


def filter_content(**kwargs):
    folders = kwargs['ti'].xcom_pull(task_ids='check_logs_content')
    folders_list = folders.replace(",latest,", "").split(',')
    unnecessary_folders = ""
    for item in folders_list:
        item_obj = item.strptime(item, "Y-%m-%d")
        if item_obj < date_today - datetime.timedelta(days=10):
            unnecessary_folders = unnecessary_folders + " " + item
    kwargs['ti'].xcom_push(key='unnecessary_folders', value=unnecessary_folders)


with DAG(
        dag_id=dag_id, default_args=args, schedule_interval="30 15 * * *"
) as dag:  # run daily at 15:30

    check_logs_content = BashOperator(
        task_id="check_logs_content",
        bash_command="ls -1 /home/airflow/airflow_services/logs/scheduler | tr '\n', ','",
        do_xcom_push=True,
        dag=dag,
    )

    filter_content = PythonOperator(
        task_id="filter_content",
        python_callable=filter_content,
        provide_context=True,
        dag=dag,
    )

    clear_logs = BashOperator(
        task_id='clear_logs',
        bash_command='rm -rf "{{ ti.xcom_pull(task_ids=\'filter_content\', key=\'unnecessary_folders\') }} "',
        depends_on_past=True,
        dag=dag,
    )

check_logs_content >> filter_content >> clear_logs
